const express = require("express");
const WebSocket = require("ws");

const app = express();

const PORT = process.env.PORT || 10000;
const SYMBOL = "BTCUSDT";
const SYMBOL_LOWER = SYMBOL.toLowerCase();

const STREAM_URL =
  `wss://fstream.binance.com/ws/${SYMBOL_LOWER}@depth@100ms`;

const SNAPSHOT_WS_URL =
  "wss://ws-fapi.binance.com/ws-fapi/v1";

const MIN_CLUSTER_USD = 500000;
const RANGE_PCT = 0.03;
const CLUSTER_GAP_PCT = 0.0002;

let book = {
  bids: new Map(),
  asks: new Map(),

  currentPrice: null,

  lastUpdateId: 0,

  initialized: false,

  websocketConnected: false,
  snapshotConnected: false,

  lastUpdate: null,

  status: "starting"
};

let depthWs = null;
let snapshotWs = null;

let reconnectTimer = null;
let snapshotTimer = null;

let requestId = 1;

// ============================================================
// HELPERS
// ============================================================

function n(value) {
  const x = Number(value);
  return Number.isFinite(x) ? x : 0;
}

function round(value, decimals = 2) {
  const factor = Math.pow(10, decimals);

  return Math.round(value * factor) / factor;
}

function usd(price, quantity) {
  return price * quantity;
}

function getCurrentPrice() {
  let bestBid = null;
  let bestAsk = null;

  for (const [price, quantity] of book.bids) {
    if (quantity <= 0) continue;

    if (bestBid === null || price > bestBid) {
      bestBid = price;
    }
  }

  for (const [price, quantity] of book.asks) {
    if (quantity <= 0) continue;

    if (bestAsk === null || price < bestAsk) {
      bestAsk = price;
    }
  }

  if (
    bestBid !== null &&
    bestAsk !== null
  ) {
    return (bestBid + bestAsk) / 2;
  }

  if (bestBid !== null) {
    return bestBid;
  }

  if (bestAsk !== null) {
    return bestAsk;
  }

  return null;
}

// ============================================================
// APPLY DEPTH UPDATE
// ============================================================

function applyUpdates(event) {
  if (!event) return;

  if (
    event.b &&
    Array.isArray(event.b)
  ) {
    for (const level of event.b) {
      const price = n(level[0]);
      const quantity = n(level[1]);

      if (!price) continue;

      if (quantity === 0) {
        book.bids.delete(price);
      } else {
        book.bids.set(
          price,
          quantity
        );
      }
    }
  }

  if (
    event.a &&
    Array.isArray(event.a)
  ) {
    for (const level of event.a) {
      const price = n(level[0]);
      const quantity = n(level[1]);

      if (!price) continue;

      if (quantity === 0) {
        book.asks.delete(price);
      } else {
        book.asks.set(
          price,
          quantity
        );
      }
    }
  }

  if (event.u) {
    book.lastUpdateId =
      n(event.u);
  }

  book.currentPrice =
    getCurrentPrice();

  book.lastUpdate =
    new Date().toISOString();
}

// ============================================================
// SNAPSHOT VIA BINANCE WEBSOCKET API
// ============================================================

function requestSnapshot() {
  if (!snapshotWs) {
    return;
  }

  if (
    snapshotWs.readyState !==
    WebSocket.OPEN
  ) {
    return;
  }

  const id = requestId++;

  const request = {
    id: String(id),

    method: "depth",

    params: {
      symbol: SYMBOL,
      limit: 1000
    }
  };

  console.log(
    "Requesting Futures depth snapshot..."
  );

  snapshotWs.send(
    JSON.stringify(request)
  );
}

function connectSnapshotWs() {
  console.log(
    "Connecting to Binance Futures WebSocket API..."
  );

  try {
    snapshotWs =
      new WebSocket(
        SNAPSHOT_WS_URL
      );

    snapshotWs.on("open", () => {
      console.log(
        "Binance Futures WebSocket API connected."
      );

      book.snapshotConnected =
        true;

      requestSnapshot();
    });

    snapshotWs.on("message", (data) => {
      try {
        const response =
          JSON.parse(
            data.toString()
          );

        if (
          response.status !== 200 ||
          !response.result
        ) {
          console.log(
            "Snapshot response:",
            JSON.stringify(
              response
            )
          );

          return;
        }

        if (
          !response.result.bids ||
          !response.result.asks
        ) {
          return;
        }

        book.bids.clear();
        book.asks.clear();

        for (
          const level of response.result.bids
        ) {
          const price =
            n(level[0]);

          const quantity =
            n(level[1]);

          if (
            price > 0 &&
            quantity > 0
          ) {
            book.bids.set(
              price,
              quantity
            );
          }
        }

        for (
          const level of response.result.asks
        ) {
          const price =
            n(level[0]);

          const quantity =
            n(level[1]);

          if (
            price > 0 &&
            quantity > 0
          ) {
            book.asks.set(
              price,
              quantity
            );
          }
        }

        book.lastUpdateId =
          n(
            response.result.lastUpdateId
          );

        book.currentPrice =
          getCurrentPrice();

        book.initialized =
          true;

        book.status =
          "live";

        book.lastUpdate =
          new Date().toISOString();

        console.log(
          "SNAPSHOT SUCCESS",
          "bids:",
          book.bids.size,
          "asks:",
          book.asks.size,
          "updateId:",
          book.lastUpdateId
        );

        console.log(
          "Current price:",
          book.currentPrice
        );

      } catch (error) {
        console.log(
          "Snapshot parse error:",
          error.message
        );
      }
    });

    snapshotWs.on("close", () => {
      console.log(
        "Snapshot WebSocket disconnected."
      );

      book.snapshotConnected =
        false;

      scheduleSnapshotReconnect();
    });

    snapshotWs.on("error", (error) => {
      console.log(
        "Snapshot WebSocket error:",
        error.message
      );
    });

  } catch (error) {
    console.log(
      "Snapshot connection error:",
      error.message
    );

    scheduleSnapshotReconnect();
  }
}

function scheduleSnapshotReconnect() {
  if (snapshotTimer) {
    return;
  }

  snapshotTimer =
    setTimeout(() => {
      snapshotTimer = null;

      connectSnapshotWs();
    }, 5000);
}

// ============================================================
// LIVE DIFF DEPTH STREAM
// ============================================================

function connectDepthWs() {
  console.log(
    "Connecting to Binance Futures depth stream..."
  );

  try {
    depthWs =
      new WebSocket(
        STREAM_URL
      );

    depthWs.on("open", () => {
      console.log(
        "Binance Futures depth stream connected."
      );

      book.websocketConnected =
        true;
    });

    depthWs.on("message", (data) => {
      try {
        const event =
          JSON.parse(
            data.toString()
          );

        if (
          event.e !==
          "depthUpdate"
        ) {
          return;
        }

        // Ignore updates until snapshot exists.
        if (!book.initialized) {
          return;
        }

        const firstUpdate =
          n(event.U);

        const finalUpdate =
          n(event.u);

        // Ignore events completely
        // older than our snapshot.
        if (
          finalUpdate <=
          book.lastUpdateId
        ) {
          return;
        }

        // Detect a gap.
        if (
          firstUpdate >
          book.lastUpdateId + 1
        ) {
          console.log(
            "ORDER BOOK GAP DETECTED"
          );

          console.log(
            "Local:",
            book.lastUpdateId
          );

          console.log(
            "Event starts:",
            firstUpdate
          );

          book.initialized =
            false;

          requestSnapshot();

          return;
        }

        applyUpdates(event);

      } catch (error) {
        console.log(
          "Depth event error:",
          error.message
        );
      }
    });

    depthWs.on("close", () => {
      console.log(
        "Depth stream disconnected."
      );

      book.websocketConnected =
        false;

      setTimeout(() => {
        connectDepthWs();
      }, 5000);
    });

    depthWs.on("error", (error) => {
      console.log(
        "Depth stream error:",
        error.message
      );
    });

  } catch (error) {
    console.log(
      "Depth connection error:",
      error.message
    );

    setTimeout(() => {
      connectDepthWs();
    }, 5000);
  }
}

// ============================================================
// CONVERT MAP TO LEVELS
// ============================================================

function getLevels(side) {
  const map =
    side === "bid"
      ? book.bids
      : book.asks;

  const levels = [];

  for (
    const [price, quantity]
    of map
  ) {
    if (
      price <= 0 ||
      quantity <= 0
    ) {
      continue;
    }

    levels.push({
      price,
      quantity,
      usd:
        usd(
          price,
          quantity
        )
    });
  }

  if (side === "bid") {
    levels.sort(
      (a, b) =>
        b.price -
        a.price
    );
  } else {
    levels.sort(
      (a, b) =>
        a.price -
        b.price
    );
  }

  return levels;
}

// ============================================================
// CLUSTERS
// ============================================================

function buildClusters(
  levels,
  side
) {
  const currentPrice =
    book.currentPrice;

  if (
    !currentPrice ||
    !levels.length
  ) {
    return [];
  }

  const range =
    currentPrice *
    RANGE_PCT;

  const filtered =
    levels.filter(
      (level) => {

        if (side === "bid") {
          return (
            level.price <
              currentPrice &&
            currentPrice -
              level.price <=
              range
          );
        }

        return (
          level.price >
            currentPrice &&
          level.price -
            currentPrice <=
            range
        );
      }
    );

  const clusters = [];

  let current = null;

  const maxGap =
    currentPrice *
    CLUSTER_GAP_PCT;

  for (
    const level of filtered
  ) {
    if (!current) {
      current = {
        side,

        priceLow:
          level.price,

        priceHigh:
          level.price,

        totalUsd:
          level.usd,

        totalQuantity:
          level.quantity,

        levels: 1,

        strongestPrice:
          level.price,

        strongestUsd:
          level.usd
      };

      continue;
    }

    const previousPrice =
      side === "bid"
        ? current.priceLow
        : current.priceHigh;

    const gap =
      Math.abs(
        level.price -
          previousPrice
      );

    if (
      gap <= maxGap
    ) {
      current.priceLow =
        Math.min(
          current.priceLow,
          level.price
        );

      current.priceHigh =
        Math.max(
          current.priceHigh,
          level.price
        );

      current.totalUsd +=
        level.usd;

      current.totalQuantity +=
        level.quantity;

      current.levels++;

      if (
        level.usd >
        current.strongestUsd
      ) {
        current.strongestUsd =
          level.usd;

        current.strongestPrice =
          level.price;
      }

    } else {
      if (
        current.totalUsd >=
        MIN_CLUSTER_USD
      ) {
        clusters.push(
          current
        );
      }

      current = {
        side,

        priceLow:
          level.price,

        priceHigh:
          level.price,

        totalUsd:
          level.usd,

        totalQuantity:
          level.quantity,

        levels: 1,

        strongestPrice:
          level.price,

        strongestUsd:
          level.usd
      };
    }
  }

  if (
    current &&
    current.totalUsd >=
      MIN_CLUSTER_USD
  ) {
    clusters.push(
      current
    );
  }

  clusters.sort(
    (a, b) =>
      b.totalUsd -
      a.totalUsd
  );

  return clusters
    .slice(0, 15)
    .map(
      (cluster) => {

        const midpoint =
          (
            cluster.priceLow +
            cluster.priceHigh
          ) / 2;

        const distance =
          (
            (
              midpoint -
              currentPrice
            ) /
            currentPrice
          ) * 100;

        return {
          side:
            cluster.side,

          priceLow:
            round(
              cluster.priceLow,
              2
            ),

          priceHigh:
            round(
              cluster.priceHigh,
              2
            ),

          midpoint:
            round(
              midpoint,
              2
            ),

          strongestPrice:
            round(
              cluster.strongestPrice,
              2
            ),

          strongestUsd:
            round(
              cluster.strongestUsd,
              2
            ),

          totalUsd:
            round(
              cluster.totalUsd,
              2
            ),

          totalQuantity:
            round(
              cluster.totalQuantity,
              6
            ),

          levels:
            cluster.levels,

          distancePct:
            round(
              distance,
              3
            )
        };
      }
    );
}

// ============================================================
// API
// ============================================================

app.get("/", (req, res) => {
  res.json({
    service:
      "Binance Futures Liquidity Relay",

    symbol: SYMBOL,

    status:
      book.status,

    initialized:
      book.initialized,

    websocketConnected:
      book.websocketConnected,

    snapshotConnected:
      book.snapshotConnected,

    currentPrice:
      book.currentPrice,

    bidLevels:
      book.bids.size,

    askLevels:
      book.asks.size,

    lastUpdateId:
      book.lastUpdateId,

    updatedAt:
      book.lastUpdate
  });
});

// ============================================================
// HEALTH
// ============================================================

app.get("/health", (req, res) => {
  res.json({
    ok: true,

    status:
      book.status,

    initialized:
      book.initialized,

    websocketConnected:
      book.websocketConnected,

    snapshotConnected:
      book.snapshotConnected,

    currentPrice:
      book.currentPrice,

    bidLevels:
      book.bids.size,

    askLevels:
      book.asks.size,

    lastUpdateId:
      book.lastUpdateId,

    updatedAt:
      book.lastUpdate
  });
});

// ============================================================
// RAW BOOK
// ============================================================

app.get("/book", (req, res) => {
  res.json({
    symbol: SYMBOL,

    currentPrice:
      book.currentPrice,

    bids:
      getLevels("bid"),

    asks:
      getLevels("ask"),

    bidLevels:
      book.bids.size,

    askLevels:
      book.asks.size,

    initialized:
      book.initialized,

    lastUpdateId:
      book.lastUpdateId,

    updatedAt:
      book.lastUpdate
  });
});

// ============================================================
// LIQUIDITY
// ============================================================

app.get("/liquidity", (req, res) => {
  const bids =
    buildClusters(
      getLevels("bid"),
      "bid"
    );

  const asks =
    buildClusters(
      getLevels("ask"),
      "ask"
    );

  const totalBuyUsd =
    bids.reduce(
      (sum, cluster) =>
        sum +
        cluster.totalUsd,
      0
    );

  const totalSellUsd =
    asks.reduce(
      (sum, cluster) =>
        sum +
        cluster.totalUsd,
      0
    );

  res.json({
    symbol: SYMBOL,

    currentPrice:
      book.currentPrice,

    buyLiquidity:
      bids,

    sellLiquidity:
      asks,

    summary: {
      buyClusters:
        bids.length,

      sellClusters:
        asks.length,

      totalBuyUsd:
        round(
          totalBuyUsd,
          2
        ),

      totalSellUsd:
        round(
          totalSellUsd,
          2
        )
    },

    book: {
      initialized:
        book.initialized,

      bidLevels:
        book.bids.size,

      askLevels:
        book.asks.size,

      lastUpdateId:
        book.lastUpdateId
    },

    connections: {
      depthWebSocket:
        book.websocketConnected,

      snapshotWebSocket:
        book.snapshotConnected
    },

    updatedAt:
      book.lastUpdate
  });
});

// ============================================================
// START
// ============================================================

app.listen(
  PORT,
  () => {

    console.log(
      `Server listening on ${PORT}`
    );

    connectDepthWs();

    connectSnapshotWs();
  }
);
