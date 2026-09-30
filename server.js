const express = require("express");
const WebSocket = require("ws");

const app = express();

const PORT = process.env.PORT || 10000;

const SYMBOL = "BTCUSDT";
const SYMBOL_LOWER = SYMBOL.toLowerCase();

const DEPTH_STREAM =
  `wss://fstream.binance.com/ws/${SYMBOL_LOWER}@depth@100ms`;

const WS_API =
  "wss://ws-fapi.binance.com/ws-fapi/v1";

// ============================================================
// LIQUIDITY SETTINGS
// ============================================================

const MIN_CLUSTER_USD = 500000;
const MIN_CLUSTER_LEVELS = 3;
const SEED_LEVEL_USD = 100000;

const PRICE_BUCKET_SIZE = 1;

const MAX_EMPTY_BUCKETS = 1;

const RANGE_PCT = 0.03;

const MAX_CLUSTERS = 12;

// ============================================================
// SNAPSHOT SAFETY
// ============================================================

// Never spam Binance with snapshot requests.

const SNAPSHOT_MIN_INTERVAL_MS =
  65000;

const SNAPSHOT_TIMEOUT_MS =
  15000;

// ============================================================
// ORDER BOOK
// ============================================================

let book = {
  bids: new Map(),
  asks: new Map(),

  currentPrice: null,

  lastUpdateId: 0,

  initialized: false,

  waitingForBridge: false,

  depthConnected: false,
  snapshotConnected: false,

  snapshotPending: false,

  status: "starting",

  resyncs: 0,

  lastUpdate: null,

  lastSnapshotRequest: null,

  lastSnapshotResponse: null,

  lastSnapshotError: null,

  snapshotRequests: 0,

  snapshot429s: 0
};

let pendingDepthEvents = [];

let depthWs = null;
let snapshotWs = null;

let depthReconnectTimer = null;
let snapshotReconnectTimer = null;

let snapshotRetryTimer = null;
let snapshotTimeoutTimer = null;

let requestId = 1;

let lastSnapshotRequestAt = 0;

// ============================================================
// PERSISTENCE
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
    const [price, quantity] of book.bids
  ) {
    if (quantity <= 0) continue;

    if (
      bestBid === null ||
      price > bestBid
    ) {
      bestBid = price;
    }
  }

  for (
    const [price, quantity] of book.asks
  ) {
    if (quantity <= 0) continue;

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

  return bestBid ?? bestAsk ?? null;
}

// ============================================================
// APPLY DEPTH
// ============================================================

function applyDepthEvent(event) {
  if (!event) return;

  if (Array.isArray(event.b)) {
    for (const level of event.b) {
      const price = num(level[0]);
      const quantity = num(level[1]);

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

  if (Array.isArray(event.a)) {
    for (const level of event.a) {
      const price = num(level[0]);
      const quantity = num(level[1]);

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

  book.lastUpdateId =
    num(event.u);

  book.currentPrice =
    getCurrentPrice();

  book.lastUpdate =
    new Date().toISOString();
}

// ============================================================
// RESET FOR SNAPSHOT
// ============================================================

function resetForSnapshot(reason = "resync") {

  book.bids.clear();
  book.asks.clear();

  book.currentPrice = null;

  book.lastUpdateId = 0;

  book.initialized = false;

  book.waitingForBridge = false;

  book.snapshotPending = false;

  book.status = "syncing";

  book.resyncs++;

  pendingDepthEvents = [];

  console.log(
    "RESET FOR SNAPSHOT:",
    reason
  );
}

// ============================================================
// SNAPSHOT RETRY TIMER
// ============================================================

function scheduleSnapshotRetry(delayMs) {

  if (snapshotRetryTimer) {
    return;
  }

  console.log(
    `Snapshot retry scheduled in ${Math.ceil(
      delayMs / 1000
    )} seconds.`
  );

  snapshotRetryTimer =
    setTimeout(
      () => {

        snapshotRetryTimer =
          null;

        requestSnapshot();

      },
      delayMs
    );
}

// ============================================================
// REQUEST SNAPSHOT
// ============================================================

function requestSnapshot() {

  // ----------------------------------------------------------
  // Already waiting for one.
  // ----------------------------------------------------------

  if (
    book.snapshotPending
  ) {
    return;
  }

  // ----------------------------------------------------------
  // WebSocket must be connected.
  // ----------------------------------------------------------

  if (!snapshotWs) {

    scheduleSnapshotRetry(
      5000
    );

    return;
  }

  if (
    snapshotWs.readyState !==
    WebSocket.OPEN
  ) {

    scheduleSnapshotRetry(
      5000
    );

    return;
  }

  // ----------------------------------------------------------
  // Rate-limit protection.
  // ----------------------------------------------------------

  const now =
    Date.now();

  const elapsed =
    now -
    lastSnapshotRequestAt;

  if (
    lastSnapshotRequestAt > 0 &&
    elapsed <
      SNAPSHOT_MIN_INTERVAL_MS
  ) {

    scheduleSnapshotRetry(
      SNAPSHOT_MIN_INTERVAL_MS -
        elapsed
    );

    return;
  }

  // ----------------------------------------------------------
  // Mark request as pending BEFORE sending.
  // ----------------------------------------------------------

  book.snapshotPending =
    true;

  book.snapshotRequests++;

  book.lastSnapshotRequest =
    new Date().toISOString();

  lastSnapshotRequestAt =
    now;

  const id =
    String(requestId++);

  const request = {
    id,

    method: "depth",

    params: {
      symbol: SYMBOL,
      limit: 1000
    }
  };

  console.log(
    "REQUESTING SNAPSHOT:",
    id
  );

  try {

    snapshotWs.send(
      JSON.stringify(request)
    );

    // --------------------------------------------------------
    // Safety timeout.
    // --------------------------------------------------------

    if (
      snapshotTimeoutTimer
    ) {
      clearTimeout(
        snapshotTimeoutTimer
      );
    }

    snapshotTimeoutTimer =
      setTimeout(
        () => {

          snapshotTimeoutTimer =
            null;

          if (
            book.snapshotPending
          ) {

            console.log(
              "SNAPSHOT RESPONSE TIMEOUT"
            );

            book.lastSnapshotError =
              "Snapshot response timeout.";

            book.snapshotPending =
              false;

            // Do NOT immediately request again.

            scheduleSnapshotRetry(
              SNAPSHOT_MIN_INTERVAL_MS
            );
          }

        },
        SNAPSHOT_TIMEOUT_MS
      );

  } catch (error) {

    book.snapshotPending =
      false;

    book.lastSnapshotError =
      error.message;

    console.log(
      "Snapshot send error:",
      error.message
    );

    scheduleSnapshotRetry(
      SNAPSHOT_MIN_INTERVAL_MS
    );
  }
}

// ============================================================
// INSTALL SNAPSHOT
// ============================================================

function installSnapshot(result) {

  if (
    !result ||
    !Array.isArray(result.bids) ||
    !Array.isArray(result.asks)
  ) {

    book.snapshotPending =
      false;

    book.lastSnapshotError =
      "Invalid snapshot response.";

    console.log(
      "Invalid snapshot."
    );

    return;
  }

  const snapshotId =
    num(result.lastUpdateId);

  if (!snapshotId) {

    book.snapshotPending =
      false;

    book.lastSnapshotError =
      "Snapshot missing lastUpdateId.";

    console.log(
      "Snapshot missing update ID."
    );

    return;
  }

  // ----------------------------------------------------------
  // Cancel timeout.
  // ----------------------------------------------------------

  if (
    snapshotTimeoutTimer
  ) {

    clearTimeout(
      snapshotTimeoutTimer
    );

    snapshotTimeoutTimer =
      null;
  }

  // ----------------------------------------------------------
  // Install snapshot.
  // ----------------------------------------------------------

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

  book.currentPrice =
    getCurrentPrice();

  book.lastSnapshotResponse =
    new Date().toISOString();

  book.lastSnapshotError =
    null;

  book.snapshotPending =
    false;

  // ----------------------------------------------------------
  // Remove events that happened before/equal to snapshot.
  // ----------------------------------------------------------

  pendingDepthEvents =
    pendingDepthEvents.filter(
      event =>
        num(event.u) >
        snapshotId
    );

  book.waitingForBridge =
    true;

  book.initialized =
    false;

  book.status =
    "waiting_for_bridge";

  console.log(
    "================================"
  );

  console.log(
    "SNAPSHOT INSTALLED"
  );

  console.log(
    "Snapshot ID:",
    snapshotId
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
    "Buffered events:",
    pendingDepthEvents.length
  );

  console.log(
    "================================"
  );

  processBufferedEvents();
}

// ============================================================
// PROCESS BUFFER
// ============================================================

function processBufferedEvents() {

  if (
    !book.waitingForBridge
  ) {
    return;
  }

  if (
    !pendingDepthEvents.length
  ) {
    return;
  }

  pendingDepthEvents.sort(
    (a, b) =>
      num(a.U) -
      num(b.U)
  );

  for (
    const event of pendingDepthEvents
  ) {

    const first =
      num(event.U);

    const last =
      num(event.u);

    // --------------------------------------------------------
    // Event entirely before snapshot.
    // --------------------------------------------------------

    if (
      last <=
      book.lastUpdateId
    ) {
      continue;
    }

    // --------------------------------------------------------
    // Correct bridge event.
    // --------------------------------------------------------

    if (
      first <=
        book.lastUpdateId + 1 &&
      last >=
        book.lastUpdateId + 1
    ) {

      applyDepthEvent(
        event
      );

      book.waitingForBridge =
        false;

      book.initialized =
        true;

      book.status =
        "live";

      book.snapshotPending =
        false;

      pendingDepthEvents = [];

      console.log(
        "================================"
      );

      console.log(
        "ORDER BOOK SYNCHRONIZED"
      );

      console.log(
        "Update ID:",
        book.lastUpdateId
      );

      console.log(
        "Price:",
        book.currentPrice
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
        "================================"
      );

      return;
    }

    // --------------------------------------------------------
    // We missed an update.
    // --------------------------------------------------------

    if (
      first >
      book.lastUpdateId + 1
    ) {

      console.log(
        "BUFFER GAP AFTER SNAPSHOT"
      );

      console.log(
        "Expected:",
        book.lastUpdateId + 1
      );

      console.log(
        "Received:",
        first,
        "-",
        last
      );

      // ------------------------------------------------------
      // IMPORTANT:
      //
      // Do not immediately hammer Binance.
      // Wait for the rate-limit-safe interval.
      // ------------------------------------------------------

      resetForSnapshot(
        "buffer gap"
      );

      scheduleSnapshotRetry(
        SNAPSHOT_MIN_INTERVAL_MS
      );

      return;
    }
  }
}

// ============================================================
// SNAPSHOT WEBSOCKET
// ============================================================

function connectSnapshot() {

  console.log(
    "Connecting snapshot WebSocket..."
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

        // ----------------------------------------------------
        // Only request if we don't already have a live book.
        // ----------------------------------------------------

        if (
          !book.initialized &&
          !book.snapshotPending
        ) {

          requestSnapshot();
        }
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

          // --------------------------------------------------
          // API error.
          // --------------------------------------------------

          if (
            response.status &&
            response.status !==
              200
          ) {

            const error =
              response.error || {};

            const code =
              num(error.code);

            const message =
              error.msg ||
              "Unknown snapshot API error.";

            console.log(
              "Snapshot API error:",
              JSON.stringify(
                response
              )
            );

            book.snapshotPending =
              false;

            book.lastSnapshotError =
              `${code}: ${message}`;

            // ------------------------------------------------
            // 429 protection.
            // ------------------------------------------------

            if (
              response.status ===
                429 ||
              code ===
                -1003
            ) {

              book.snapshot429s++;

              console.log(
                "SNAPSHOT RATE LIMITED."
              );

              console.log(
                "Backing off for 65 seconds."
              );

              scheduleSnapshotRetry(
                SNAPSHOT_MIN_INTERVAL_MS
              );

              return;
            }

            // Other errors:
            // wait before trying again.

            scheduleSnapshotRetry(
              30000
            );

            return;
          }

          // --------------------------------------------------
          // Valid result.
          // --------------------------------------------------

          if (
            response.result
          ) {

            installSnapshot(
              response.result
            );
          }

        } catch (error) {

          console.log(
            "Snapshot parse error:",
            error.message
          );

          book.snapshotPending =
            false;

          book.lastSnapshotError =
            error.message;

          scheduleSnapshotRetry(
            30000
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

        book.snapshotPending =
          false;

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
    );

    snapshotWs.on(
      "error",
      (error) => {

        console.log(
          "Snapshot WebSocket error:",
          error.message
        );

        book.lastSnapshotError =
          error.message;
      }
    );

  } catch (error) {

    console.log(
      "Snapshot connection error:",
      error.message
    );

    book.snapshotConnected =
      false;

    book.snapshotPending =
      false;

    setTimeout(
      connectSnapshot,
      5000
    );
  }
}

// ============================================================
// DEPTH WEBSOCKET
// ============================================================

function connectDepth() {

  console.log(
    "Connecting depth WebSocket..."
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
          "Depth WebSocket connected."
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

          const first =
            num(event.U);

          const last =
            num(event.u);

          // ------------------------------------------------
          // Before synchronization:
          // buffer events.
          // ------------------------------------------------

          if (
            !book.initialized
          ) {

            pendingDepthEvents.push(
              event
            );

            if (
              pendingDepthEvents.length >
              10000
            ) {

              pendingDepthEvents =
                pendingDepthEvents.slice(
                  -5000
                );
            }

            processBufferedEvents();

            return;
          }

          // ------------------------------------------------
          // Already processed.
          // ------------------------------------------------

          if (
            last <=
            book.lastUpdateId
          ) {
            return;
          }

          // ------------------------------------------------
          // Normal sequential update.
          // ------------------------------------------------

          if (
            first <=
              book.lastUpdateId + 1 &&
            last >=
              book.lastUpdateId + 1
          ) {

            applyDepthEvent(
              event
            );

            return;
          }

          // ------------------------------------------------
          // Real live gap.
          // ------------------------------------------------

          if (
            first >
            book.lastUpdateId + 1
          ) {

            console.log(
              "================================"
            );

            console.log(
              "LIVE DEPTH GAP"
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

            console.log(
              "ORDER BOOK WILL RESYNC"
            );

            console.log(
              "================================"
            );

            resetForSnapshot(
              "live depth gap"
            );

            scheduleSnapshotRetry(
              SNAPSHOT_MIN_INTERVAL_MS
            );

            return;
          }

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
          "Depth WebSocket closed."
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
          "Depth WebSocket error:",
          error.message
        );
      }
    );

  } catch (error) {

    console.log(
      "Depth connection error:",
      error.message
    );

    book.depthConnected =
      false;

    setTimeout(
      connectDepth,
      5000
    );
  }
}

// ============================================================
// LEVELS
// ============================================================

function getLevels(side) {

  const source =
    side === "bid"
      ? book.bids
      : book.asks;

  const levels = [];

  for (
    const [price, quantity]
    of source
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
        price * quantity
    });
  }

  if (
    side === "bid"
  ) {

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
// BUCKETS
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

    const index =
      Math.floor(
        level.price /
          PRICE_BUCKET_SIZE
      );

    let bucket =
      buckets.get(index);

    if (!bucket) {

      bucket = {

        index,

        low:
          index *
          PRICE_BUCKET_SIZE,

        high:
          (index + 1) *
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
        index,
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
// CLUSTERS
// ============================================================

function buildClusters(
  levels,
  side
) {

  const buckets =
    createBuckets(
      levels,
      side
    );

  if (!buckets.length) {
    return [];
  }

  const strong =
    buckets.filter(
      bucket =>
        bucket.totalUsd >=
          MIN_CLUSTER_USD &&
        bucket.seedLevels >= 1
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

        seedLevels:
          bucket.seedLevels,

        strongestPrice:
          bucket.strongestPrice,

        strongestUsd:
          bucket.strongestUsd,

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
      cluster => {

        const midpoint =
          (
            cluster.low +
            cluster.high
          ) / 2;

        const distancePct =
          (
            (
              midpoint -
              book.currentPrice
            ) /
            book.currentPrice
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

  for (
    const cluster of clusters
  ) {

    let match =
      clusterHistory[
        side
      ].find(
        old => {

          const difference =
            Math.abs(
              old.midpoint -
              cluster.midpoint
            );

          return (
            difference /
              cluster.midpoint <=
            0.0005
          );
        }
      );

    if (match) {

      match.lastSeen =
        now;

      match.observations++;

      match.lastUsd =
        cluster.totalUsd;

      cluster.persistence =
        match.observations;

    } else {

      match = {

        midpoint:
          cluster.midpoint,

        firstSeen:
          now,

        lastSeen:
          now,

        observations:
          1,

        lastUsd:
          cluster.totalUsd
      };

      clusterHistory[
        side
      ].push(
        match
      );

      cluster.persistence =
        1;
    }
  }

  clusterHistory[
    side
  ] =
    clusterHistory[
      side
    ].filter(
      item =>
        now -
          item.lastSeen <
        10 * 60 * 1000
    );
}

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

      waitingForBridge:
        book.waitingForBridge,

      snapshotPending:
        book.snapshotPending,

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

      pendingEvents:
        pendingDepthEvents.length,

      lastUpdateId:
        book.lastUpdateId,

      resyncs:
        book.resyncs,

      snapshotRequests:
        book.snapshotRequests,

      snapshot429s:
        book.snapshot429s,

      lastSnapshotRequest:
        book.lastSnapshotRequest,

      lastSnapshotResponse:
        book.lastSnapshotResponse,

      lastSnapshotError:
        book.lastSnapshotError,

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

    if (
      !book.initialized
    ) {

      res.json({

        symbol:
          SYMBOL,

        currentPrice:
          book.currentPrice,

        status:
          book.status,

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

        connections: {

          depthWebSocket:
            book.depthConnected,

          snapshotWebSocket:
            book.snapshotConnected
        },

        snapshot: {

          pending:
            book.snapshotPending,

          requests:
            book.snapshotRequests,

          rateLimited:
            book.snapshot429s,

          lastError:
            book.lastSnapshotError
        },

        message:
          "Order book is still synchronizing."
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

      waitingForBridge:
        book.waitingForBridge,

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

      waitingForBridge:
        book.waitingForBridge,

      snapshotPending:
        book.snapshotPending,

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

      pendingEvents:
        pendingDepthEvents.length,

      lastUpdateId:
        book.lastUpdateId,

      resyncs:
        book.resyncs,

      snapshotRequests:
        book.snapshotRequests,

      snapshot429s:
        book.snapshot429s,

      lastSnapshotError:
        book.lastSnapshotError,

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
      `Binance Futures Liquidity Relay running on port ${PORT}`
    );

    resetForSnapshot(
      "startup"
    );

    connectDepth();

    connectSnapshot();
  }
);
