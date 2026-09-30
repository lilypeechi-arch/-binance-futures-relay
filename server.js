const express = require("express");
const WebSocket = require("ws");

const app = express();

const PORT = process.env.PORT || 10000;

const SYMBOL = "BTCUSDT";
const SYMBOL_LOWER = SYMBOL.toLowerCase();

// ============================================================
// BINANCE CONNECTIONS
// ============================================================

const DEPTH_STREAM =
  `wss://fstream.binance.com/ws/${SYMBOL_LOWER}@depth@100ms`;

const WS_API =
  "wss://ws-fapi.binance.com/ws-fapi/v1";

// ============================================================
// LIQUIDITY SETTINGS
// ============================================================

const MIN_CLUSTER_USD = 500000;

// A cluster must contain at least this many
// individual order-book levels.
const MIN_CLUSTER_LEVELS = 3;

// Minimum individual level size used as a
// concentration seed.
const SEED_LEVEL_USD = 100000;

// Price bucket size.
const PRICE_BUCKET_SIZE = 1;

// Maximum empty $1 buckets allowed between
// strong buckets before starting a new cluster.
const MAX_EMPTY_BUCKETS = 1;

// Search 3% above and below current price.
const RANGE_PCT = 0.03;

// Maximum clusters returned per side.
const MAX_CLUSTERS = 12;

// How long a cluster remains in persistence memory.
const PERSISTENCE_MAX_AGE_MS =
  10 * 60 * 1000;

// Price matching tolerance for persistence.
const PERSISTENCE_TOLERANCE_PCT = 0.0005;

// ============================================================
// ORDER BOOK STATE
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

let pendingDepthEvents = [];

let depthWs = null;
let snapshotWs = null;

let depthReconnectTimer = null;
let snapshotReconnectTimer = null;

let requestId = 1;

// ============================================================
// PERSISTENCE STATE
// ============================================================

let clusterHistory = {
  bid: [],
  ask: []
};

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
    Math.round(value * factor) /
    factor
  );
}

function getCurrentPrice() {
  let bestBid = null;
  let bestAsk = null;

  for (
    const [price, quantity]
    of book.bids
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
    const [price, quantity]
    of book.asks
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
      bestBid + bestAsk
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
      num(event.u);
  }

  book.currentPrice =
    getCurrentPrice();

  book.lastUpdate =
    new Date().toISOString();
}

// ============================================================
// RESET / RESYNC
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
// SNAPSHOT REQUEST
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
      JSON.stringify(request)
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
      "Invalid snapshot."
    );

    return;
  }

  const snapshotId =
    num(result.lastUpdateId);

  if (!snapshotId) {
    console.log(
      "Snapshot has no update ID."
    );

    return;
  }

  const events =
    pendingDepthEvents
      .slice()
      .sort(
        (a, b) =>
          num(a.u) -
          num(b.u)
      );

  let bridgeIndex = -1;

  for (
    let i = 0;
    i < events.length;
    i++
  ) {
    const first =
      num(events[i].U);

    const last =
      num(events[i].u);

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

  if (bridgeIndex >= 0) {
    for (
      let i = bridgeIndex;
      i < events.length;
      i++
    ) {
      const event =
        events[i];

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
          "Gap during synchronization."
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
    "================================"
  );

  console.log(
    "BOOK SYNCHRONIZED"
  );

  console.log(
    "Update ID:",
    book.lastUpdateId
  );

  console.log(
    "Bids:",
    book.bids.size
  );

  console.log(
    "Asks:",
    book.asks.size
  );

  console.log(
    "Price:",
    book.currentPrice
  );

  console.log(
    "================================"
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
              "Snapshot response:",
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

          if (
            !book.initialized
          ) {
            pendingDepthEvents.push(
              event
            );

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

          if (
            last <=
            book.lastUpdateId
          ) {
            return;
          }

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

        if (
          depthReconnectTimer
        ) {
          return;
        }

        depthReconnectTimer =
          setTimeout(
            () => {
              depthReconnectTimer =
                null;

              connectDepth();
            },
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
// GET BOOK LEVELS
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

    const value =
      price * quantity;

    levels.push({
      price,

      quantity,

      usd: value
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
// CREATE PRICE BUCKETS
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
    if (
      side === "bid"
    ) {
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
    }

    if (
      side === "ask"
    ) {
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

        seedLevels: 0,

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
      level.usd >=
      SEED_LEVEL_USD
    ) {
      bucket.seedLevels++;
    }

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
// BUILD CLUSTERS
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

  // Only buckets containing meaningful
  // liquidity can participate.
  const strongBuckets =
    buckets.filter(
      (bucket) =>
        bucket.totalUsd >=
          MIN_CLUSTER_USD &&
        bucket.seedLevels >= 1
    );

  if (
    !strongBuckets.length
  ) {
    return [];
  }

  const clusters = [];

  let current = null;

  for (
    const bucket of strongBuckets
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

        seedLevels:
          bucket.seedLevels,

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

      current.seedLevels +=
        bucket.seedLevels;

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
      if (
        current.levels >=
        MIN_CLUSTER_LEVELS
      ) {
        clusters.push(
          current
        );
      }

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

        seedLevels:
          bucket.seedLevels,

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

  if (
    current &&
    current.levels >=
      MIN_CLUSTER_LEVELS
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

          seedLevels:
            cluster.seedLevels,

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
// PERSISTENCE
// ============================================================

function updatePersistence(
  clusters,
  side
) {
  const now =
    Date.now();

  const history =
    clusterHistory[side];

  const results = [];

  for (
    const cluster of clusters
  ) {
    const existing =
      history.find(
        (old) => {

          const oldMid =
            old.midpoint;

          const newMid =
            cluster.midpoint;

          const difference =
            Math.abs(
              newMid -
              oldMid
            );

          return (
            difference /
              newMid <=
            PERSISTENCE_TOLERANCE_PCT
          );
        }
      );

    if (existing) {
      existing.lastSeen =
        now;

      existing.observations++;

      existing.lastUsd =
        cluster.totalUsd;

      existing.lastPrice =
        cluster.midpoint;

      cluster.persistence =
        existing.observations;

      cluster.firstSeen =
        new Date(
          existing.firstSeen
        ).toISOString();

      cluster.lastSeen =
        new Date(
          existing.lastSeen
        ).toISOString();

    } else {
      const newRecord = {
        midpoint:
          cluster.midpoint,

        firstSeen:
          now,

        lastSeen:
          now,

        observations:
          1,

        lastUsd:
          cluster.totalUsd,

        lastPrice:
          cluster.midpoint
      };

      history.push(
        newRecord
      );

      cluster.persistence =
        1;

      cluster.firstSeen =
        new Date(
          now
        ).toISOString();

      cluster.lastSeen =
        new Date(
          now
        ).toISOString();
    }
  }

  clusterHistory[side] =
    history.filter(
      (item) =>
        now -
          item.lastSeen <=
        PERSISTENCE_MAX_AGE_MS
    );

  return results;
}

// ============================================================
// LIQUIDITY ENDPOINT
// ============================================================

app.get(
  "/liquidity",
  (req, res) => {

    if (
      !book.initialized
    ) {
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

    const buy =
      buildClusters(
        getLevels("bid"),
        "bid"
      );

    const sell =
      buildClusters(
        getLevels("ask"),
        "ask"
      );

    updatePersistence(
      buy,
      "bid"
    );

    updatePersistence(
      sell,
      "ask"
    );

    const totalBuyUsd =
      buy.reduce(
        (sum, cluster) =>
          sum +
          cluster.totalUsd,
        0
      );

    const totalSellUsd =
      sell.reduce(
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
        buy,

      sellLiquidity:
        sell,

      summary: {
        buyClusters:
          buy.length,

        sellClusters:
          sell.length,

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

      settings: {
        minClusterUsd:
          MIN_CLUSTER_USD,

        minClusterLevels:
          MIN_CLUSTER_LEVELS,

        seedLevelUsd:
          SEED_LEVEL_USD,

        rangePct:
          RANGE_PCT,

        priceBucketSize:
          PRICE_BUCKET_SIZE
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
// START SERVER
// ============================================================

app.listen(
  PORT,
  () => {

    console.log(
      `Binance Futures Liquidity Relay running on port ${PORT}`
    );

    resetBookForResync();

    connectDepth();

    connectSnapshot();
  }
);
