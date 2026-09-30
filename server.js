const express = require("express");
const WebSocket = require("ws");

const app = express();

const PORT = process.env.PORT || 10000;

const SYMBOL = "BTCUSDT";
const SYMBOL_LOWER = SYMBOL.toLowerCase();

// ============================================================
// BINANCE
// ============================================================

const DEPTH_STREAM =
  `wss://fstream.binance.com/ws/${SYMBOL_LOWER}@depth@100ms`;

const WS_API =
  "wss://ws-fapi.binance.com/ws-fapi/v1";

// ============================================================
// LIQUIDITY SETTINGS
// ============================================================

// Minimum USD required for a liquidity cluster
const MIN_CLUSTER_USD = 500000;

// Minimum USD at one price level to be considered a
// significant liquidity seed.
const SEED_LEVEL_USD = 100000;

// Price bucket size.
// Example: $1 BTC price buckets.
const PRICE_BUCKET_SIZE = 1;

// Neighboring buckets can be combined when they are
// directly adjacent.
const MAX_EMPTY_BUCKETS = 1;

// Only inspect this percentage away from current price.
const RANGE_PCT = 0.03;

// Maximum clusters returned per side.
const MAX_CLUSTERS = 12;

// ============================================================
// STATE
// ============================================================

let book = {
  bids: new Map(),
  asks: new Map(),

  currentPrice: null,

  lastUpdateId: 0,

  initialized: false,

  depthConnected: false,
  snapshotConnected: false,

  snapshotPending: false,

  lastUpdate: null,

  status: "starting",

  resyncs: 0
};

// Events received while waiting for snapshot.
let pendingDepthEvents = [];

let depthWs = null;
let snapshotWs = null;

let depthReconnectTimer = null;
let snapshotReconnectTimer = null;

let requestId = 1;

// ============================================================
// HELPERS
// ============================================================

function num(value) {
  const n = Number(value);

  return Number.isFinite(n)
    ? n
    : 0;
}

function round(value, decimals = 2) {
  const factor =
    Math.pow(10, decimals);

  return (
    Math.round(
      value * factor
    ) / factor
  );
}

function getCurrentPrice() {
  let bestBid = null;
  let bestAsk = null;

  for (
    const [
      price,
      quantity
    ] of book.bids
  ) {
    if (quantity <= 0) {
      continue;
    }

    if (
      bestBid === null ||
      price > bestBid
    ) {
      bestBid = price;
    }
  }

  for (
    const [
      price,
      quantity
    ] of book.asks
  ) {
    if (quantity <= 0) {
      continue;
    }

    if (
      bestAsk === null ||
      price < bestAsk
    ) {
      bestAsk = price;
    }
  }

  if (
    bestBid !== null &&
    bestAsk !== null
  ) {
    return (
      bestBid +
      bestAsk
    ) / 2;
  }

  return (
    bestBid ??
    bestAsk ??
    null
  );
}

// ============================================================
// APPLY DEPTH EVENT
// ============================================================

function applyDepthEvent(event) {
  if (!event) {
    return;
  }

  if (
    Array.isArray(event.b)
  ) {
    for (
      const level of event.b
    ) {
      const price =
        num(level[0]);

      const quantity =
        num(level[1]);

      if (!price) {
        continue;
      }

      if (quantity === 0) {
        book.bids.delete(
          price
        );
      } else {
        book.bids.set(
          price,
          quantity
        );
      }
    }
  }

  if (
    Array.isArray(event.a)
  ) {
    for (
      const level of event.a
    ) {
      const price =
        num(level[0]);

      const quantity =
        num(level[1]);

      if (!price) {
        continue;
      }

      if (quantity === 0) {
        book.asks.delete(
          price
        );
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
      num(event.u);
  }

  book.currentPrice =
    getCurrentPrice();

  book.lastUpdate =
    new Date().toISOString();
}

// ============================================================
// RESET BOOK
// ============================================================

function resetBookForResync() {
  book.bids.clear();
  book.asks.clear();

  book.currentPrice = null;

  book.lastUpdateId = 0;

  book.initialized = false;

  book.snapshotPending = true;

  book.status = "syncing";

  book.resyncs++;

  pendingDepthEvents = [];
}

// ============================================================
// REQUEST SNAPSHOT
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

  if (book.snapshotPending) {
    // Already waiting for one.
    // Still okay to request the first snapshot
    // during initial startup.
  }

  const id =
    requestId++;

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

  try {
    snapshotWs.send(
      JSON.stringify(
        request
      )
    );
  } catch (error) {
    console.log(
      "Snapshot request error:",
      error.message
    );
  }
}

// ============================================================
// PROCESS SNAPSHOT
// ============================================================

function processSnapshot(result) {
  if (
    !result ||
    !Array.isArray(result.bids) ||
    !Array.isArray(result.asks)
  ) {
    console.log(
      "Invalid snapshot received."
    );

    return;
  }

  const snapshotId =
    num(
      result.lastUpdateId
    );

  if (!snapshotId) {
    console.log(
      "Snapshot has no lastUpdateId."
    );

    return;
  }

  const oldPending =
    pendingDepthEvents
      .slice()
      .sort(
        (a, b) =>
          num(a.u) -
          num(b.u)
      );

  // Find the first event that bridges
  // the snapshot.
  let bridgeIndex = -1;

  for (
    let i = 0;
    i < oldPending.length;
    i++
  ) {
    const event =
      oldPending[i];

    const first =
      num(event.U);

    const last =
      num(event.u);

    if (
      first <=
        snapshotId + 1 &&
      last >=
        snapshotId + 1
    ) {
      bridgeIndex = i;

      break;
    }
  }

  // Replace the book with snapshot.
  book.bids.clear();
  book.asks.clear();

  for (
    const level of result.bids
  ) {
    const price =
      num(level[0]);

    const quantity =
      num(level[1]);

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
    const level of result.asks
  ) {
    const price =
      num(level[0]);

    const quantity =
      num(level[1]);

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
    snapshotId;

  // If we have a bridging event,
  // apply it and all later events.
  if (bridgeIndex >= 0) {
    for (
      let i = bridgeIndex;
      i < oldPending.length;
      i++
    ) {
      const event =
        oldPending[i];

      const first =
        num(event.U);

      const last =
        num(event.u);

      if (
        last <=
        book.lastUpdateId
      ) {
        continue;
      }

      if (
        first >
        book.lastUpdateId + 1
      ) {
        console.log(
          "Gap found during snapshot synchronization."
        );

        resetBookForResync();

        setTimeout(
          requestSnapshot,
          250
        );

        return;
      }

      applyDepthEvent(
        event
      );
    }
  }

  pendingDepthEvents = [];

  book.currentPrice =
    getCurrentPrice();

  book.initialized = true;

  book.snapshotPending =
    false;

  book.status = "live";

  book.lastUpdate =
    new Date().toISOString();

  console.log(
    "BOOK SYNCHRONIZED"
  );

  console.log(
    "Snapshot update ID:",
    snapshotId
  );

  console.log(
    "Current update ID:",
    book.lastUpdateId
  );

  console.log(
    "Bid levels:",
    book.bids.size
  );

  console.log(
    "Ask levels:",
    book.asks.size
  );

  console.log(
    "Current price:",
    book.currentPrice
  );
}

// ============================================================
// SNAPSHOT WEBSOCKET
// ============================================================

function connectSnapshot() {
  console.log(
    "Connecting to Binance Futures WebSocket API..."
  );

  try {
    snapshotWs =
      new WebSocket(
        WS_API
      );

    snapshotWs.on(
      "open",
      () => {
        console.log(
          "Snapshot WebSocket connected."
        );

        book.snapshotConnected =
          true;

        requestSnapshot();
      }
    );

    snapshotWs.on(
      "message",
      (data) => {
        try {
          const response =
            JSON.parse(
              data.toString()
            );

          if (
            response.status !==
            200
          ) {
            console.log(
              "Snapshot API response:",
              JSON.stringify(
                response
              )
            );

            return;
          }

          if (
            !response.result
          ) {
            return;
          }

          processSnapshot(
            response.result
          );

        } catch (error) {
          console.log(
            "Snapshot parse error:",
            error.message
          );
        }
      }
    );

    snapshotWs.on(
      "close",
      () => {
        console.log(
          "Snapshot WebSocket closed."
        );

        book.snapshotConnected =
          false;

        scheduleSnapshotReconnect();
      }
    );

    snapshotWs.on(
      "error",
      (error) => {
        console.log(
          "Snapshot WebSocket error:",
          error.message
        );
      }
    );

  } catch (error) {
    console.log(
      "Snapshot connection error:",
      error.message
    );

    scheduleSnapshotReconnect();
  }
}

function scheduleSnapshotReconnect() {
  if (
    snapshotReconnectTimer
  ) {
    return;
  }

  snapshotReconnectTimer =
    setTimeout(
      () => {
        snapshotReconnectTimer =
          null;

        connectSnapshot();
      },
      5000
    );
}

// ============================================================
// LIVE DEPTH WEBSOCKET
// ============================================================

function connectDepth() {
  console.log(
    "Connecting to Binance Futures depth stream..."
  );

  try {
    depthWs =
      new WebSocket(
        DEPTH_STREAM
      );

    depthWs.on(
      "open",
      () => {
        console.log(
          "Depth stream connected."
        );

        book.depthConnected =
          true;
      }
    );

    depthWs.on(
      "message",
      (data) => {
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

          // While waiting for the snapshot,
          // keep the events so we can synchronize
          // correctly afterward.
          if (
            !book.initialized
          ) {
            pendingDepthEvents.push(
              event
            );

            // Prevent unlimited memory growth.
            if (
              pendingDepthEvents.length >
              5000
            ) {
              pendingDepthEvents =
                pendingDepthEvents.slice(
                  -2500
                );
            }

            return;
          }

          const first =
            num(event.U);

          const last =
            num(event.u);

          // Already processed.
          if (
            last <=
            book.lastUpdateId
          ) {
            return;
          }

          // We missed one or more events.
          if (
            first >
            book.lastUpdateId + 1
          ) {
            console.log(
              "DEPTH GAP DETECTED"
            );

            console.log(
              "Local:",
              book.lastUpdateId
            );

            console.log(
              "Incoming:",
              first,
              "-",
              last
            );

            resetBookForResync();

            requestSnapshot();

            return;
          }

          applyDepthEvent(
            event
          );

        } catch (error) {
          console.log(
            "Depth event error:",
            error.message
          );
        }
      }
    );

    depthWs.on(
      "close",
      () => {
        console.log(
          "Depth stream closed."
        );

        book.depthConnected =
          false;

        setTimeout(
          connectDepth,
          5000
        );
      }
    );

    depthWs.on(
      "error",
      (error) => {
        console.log(
          "Depth stream error:",
          error.message
        );
      }
    );

  } catch (error) {
    console.log(
      "Depth connection error:",
      error.message
    );

    setTimeout(
      connectDepth,
      5000
    );
  }
}

// ============================================================
// LEVEL EXTRACTION
// ============================================================

function getLevels(side) {
  const source =
    side === "bid"
      ? book.bids
      : book.asks;

  const levels = [];

  for (
    const [
      price,
      quantity
    ] of source
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
        price *
        quantity
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
// PRICE BUCKETS
// ============================================================

function createBuckets(
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

  const buckets =
    new Map();

  for (
    const level of levels
  ) {
    if (side === "bid") {
      if (
        level.price >=
        currentPrice
      ) {
        continue;
      }

      if (
        currentPrice -
          level.price >
        range
      ) {
        continue;
      }
    } else {
      if (
        level.price <=
        currentPrice
      ) {
        continue;
      }

      if (
        level.price -
          currentPrice >
        range
      ) {
        continue;
      }
    }

    const bucketIndex =
      Math.floor(
        level.price /
          PRICE_BUCKET_SIZE
      );

    let bucket =
      buckets.get(
        bucketIndex
      );

    if (!bucket) {
      bucket = {
        index:
          bucketIndex,

        low:
          bucketIndex *
          PRICE_BUCKET_SIZE,

        high:
          (
            bucketIndex + 1
          ) *
          PRICE_BUCKET_SIZE,

        totalUsd: 0,

        totalQuantity: 0,

        levels: 0,

        strongestPrice:
          level.price,

        strongestUsd:
          level.usd
      };

      buckets.set(
        bucketIndex,
        bucket
      );
    }

    bucket.totalUsd +=
      level.usd;

    bucket.totalQuantity +=
      level.quantity;

    bucket.levels++;

    if (
      level.usd >
      bucket.strongestUsd
    ) {
      bucket.strongestUsd =
        level.usd;

      bucket.strongestPrice =
        level.price;
    }
  }

  return Array.from(
    buckets.values()
  ).sort(
    (a, b) =>
      a.index -
      b.index
  );
}

// ============================================================
// BUILD CONCENTRATED CLUSTERS
// ============================================================

function buildClusters(
  levels,
  side
) {
  const currentPrice =
    book.currentPrice;

  if (!currentPrice) {
    return [];
  }

  const buckets =
    createBuckets(
      levels,
      side
    );

  if (!buckets.length) {
    return [];
  }

  // Only buckets with meaningful liquidity
  // can start a cluster.
  const strong =
    buckets.filter(
      (bucket) =>
        bucket.totalUsd >=
        MIN_CLUSTER_USD
    );

  if (!strong.length) {
    return [];
  }

  const clusters = [];

  let current = null;

  for (
    const bucket of strong
  ) {
    if (!current) {
      current = {
        low:
          bucket.low,

        high:
          bucket.high,

        totalUsd:
          bucket.totalUsd,

        totalQuantity:
          bucket.totalQuantity,

        levels:
          bucket.levels,

        strongestPrice:
          bucket.strongestPrice,

        strongestUsd:
          bucket.strongestUsd,

        firstIndex:
          bucket.index,

        lastIndex:
          bucket.index
      };

      continue;
    }

    const gap =
      bucket.index -
      current.lastIndex -
      1;

    if (
      gap <=
      MAX_EMPTY_BUCKETS
    ) {
      current.high =
        bucket.high;

      current.totalUsd +=
        bucket.totalUsd;

      current.totalQuantity +=
        bucket.totalQuantity;

      current.levels +=
        bucket.levels;

      current.lastIndex =
        bucket.index;

      if (
        bucket.strongestUsd >
        current.strongestUsd
      ) {
        current.strongestUsd =
          bucket.strongestUsd;

        current.strongestPrice =
          bucket.strongestPrice;
      }

    } else {
      clusters.push(
        current
      );

      current = {
        low:
          bucket.low,

        high:
          bucket.high,

        totalUsd:
          bucket.totalUsd,

        totalQuantity:
          bucket.totalQuantity,

        levels:
          bucket.levels,

        strongestPrice:
          bucket.strongestPrice,

        strongestUsd:
          bucket.strongestUsd,

        firstIndex:
          bucket.index,

        lastIndex:
          bucket.index
      };
    }
  }

  if (current) {
    clusters.push(
      current
    );
  }

  // Sort by actual liquidity.
  clusters.sort(
    (a, b) =>
      b.totalUsd -
      a.totalUsd
  );

  return clusters
    .slice(
      0,
      MAX_CLUSTERS
    )
    .map(
      (cluster) => {

        const midpoint =
          (
            cluster.low +
            cluster.high
          ) / 2;

        const distancePct =
          (
            (
              midpoint -
              currentPrice
            ) /
            currentPrice
          ) *
          100;

        return {
          side,

          priceLow:
            round(
              cluster.low,
              2
            ),

          priceHigh:
            round(
              cluster.high,
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
              distancePct,
              3
            )
        };
      }
    );
}

// ============================================================
// ROOT
// ============================================================

app.get(
  "/",
  (req, res) => {

    res.json({
      service:
        "Binance Futures Liquidity Relay",

      symbol:
        SYMBOL,

      status:
        book.status,

      initialized:
        book.initialized,

      depthConnected:
        book.depthConnected,

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

      resyncs:
        book.resyncs,

      updatedAt:
        book.lastUpdate
    });
  }
);

// ============================================================
// HEALTH
// ============================================================

app.get(
  "/health",
  (req, res) => {

    res.json({
      ok: true,

      symbol:
        SYMBOL,

      status:
        book.status,

      initialized:
        book.initialized,

      depthConnected:
        book.depthConnected,

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

      resyncs:
        book.resyncs,

      updatedAt:
        book.lastUpdate
    });
  }
);

// ============================================================
// RAW BOOK
// ============================================================

app.get(
  "/book",
  (req, res) => {

    res.json({
      symbol:
        SYMBOL,

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
  }
);

// ============================================================
// LIQUIDITY
// ============================================================

app.get(
  "/liquidity",
  (req, res) => {

    // Do NOT report clusters from an
    // unsynchronized book.
    if (!book.initialized) {

      res.json({
        symbol:
          SYMBOL,

        currentPrice:
          book.currentPrice,

        status:
          "syncing",

        buyLiquidity: [],

        sellLiquidity: [],

        summary: {
          buyClusters: 0,

          sellClusters: 0,

          totalBuyUsd: 0,

          totalSellUsd: 0
        },

        book: {
          initialized:
            false,

          bidLevels:
            book.bids.size,

          askLevels:
            book.asks.size,

          lastUpdateId:
            book.lastUpdateId
        },

        message:
          "Waiting for synchronized order book."
      });

      return;
    }

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
      symbol:
        SYMBOL,

      currentPrice:
        round(
          book.currentPrice,
          2
        ),

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
          true,

        bidLevels:
          book.bids.size,

        askLevels:
          book.asks.size,

        lastUpdateId:
          book.lastUpdateId
      },

      connections: {
        depthWebSocket:
          book.depthConnected,

        snapshotWebSocket:
          book.snapshotConnected
      },

      updatedAt:
        book.lastUpdate
    });
  }
);

// ============================================================
// START
// ============================================================

app.listen(
  PORT,
  () => {

    console.log(
      `Relay running on port ${PORT}`
    );

    resetBookForResync();

    connectDepth();

    connectSnapshot();
  }
);
