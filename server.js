const express = require("express");
const WebSocket = require("ws");

const app = express();

const PORT = process.env.PORT || 10000;
const SYMBOL = "BTCUSDT";
const STREAM_SYMBOL = SYMBOL.toLowerCase();

const DEPTH_WS =
  `wss://fstream.binance.com/ws/${STREAM_SYMBOL}@depth@100ms`;

const WS_API =
  "wss://ws-fapi.binance.com/ws-fapi/v1";

// ============================================================
// SETTINGS
// ============================================================

// Snapshot protection
const SNAPSHOT_MIN_INTERVAL_MS = 65000;
const SNAPSHOT_TIMEOUT_MS = 15000;

// Order-book scanning
const RANGE_PCT = 0.03;

// Minimum USD required for an individual liquidity level
const SEED_LEVEL_USD = 100000;

// Minimum total USD for a cluster
const MIN_CLUSTER_USD = 500000;

// Minimum number of meaningful levels in a cluster
const MIN_CLUSTER_LEVELS = 3;

// How far around a strong liquidity level we combine orders
// Example: 5 means strongest level +/- $5
const CLUSTER_RADIUS_USD = 5;

// Minimum USD for a level to contribute to a cluster
const MIN_CLUSTER_LEVEL_USD = 50000;

// Minimum separation between cluster centers
const MIN_CLUSTER_SEPARATION_USD = 10;

// Maximum clusters returned per side
const MAX_CLUSTERS = 12;

// Maximum buffered websocket events
const MAX_PENDING_EVENTS = 20000;

// ============================================================
// LOCAL ORDER BOOK
// ============================================================

const bids = new Map();
const asks = new Map();

let currentPrice = null;

let lastUpdateId = 0;

let initialized = false;

let waitingForBridge = false;

let snapshotPending = false;

let gapRecovery = false;

let gapDetected = false;

let pendingDepthEvents = [];

let depthWs = null;
let snapshotWs = null;

let reconnectTimer = null;
let snapshotTimer = null;

let updatedAt = null;

let lastSnapshotRequest = null;
let lastSnapshotResponse = null;
let lastSnapshotError = null;

let snapshotRequests = 0;
let snapshot429s = 0;

let resyncs = 0;

let bridgeAttempts = 0;
let bridgeFound = 0;

let gapCount = 0;

let lastGapLocal = null;
let lastGapIncoming = null;

let lastResetReason = null;

let lastSnapshotId = null;

// ============================================================
// UTILITY
// ============================================================

function nowIso() {
  return new Date().toISOString();
}

function toNumber(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
}

function safeClose(ws) {
  try {
    if (ws) ws.close();
  } catch (_) {}
}

function clearReconnectTimer() {
  if (reconnectTimer) {
    clearTimeout(reconnectTimer);
    reconnectTimer = null;
  }
}

function clearSnapshotTimer() {
  if (snapshotTimer) {
    clearTimeout(snapshotTimer);
    snapshotTimer = null;
  }
}

// ============================================================
// ORDER BOOK RESET
// ============================================================

function clearBook() {
  bids.clear();
  asks.clear();

  lastUpdateId = 0;
  initialized = false;
  waitingForBridge = false;

  pendingDepthEvents = [];

  currentPrice = null;
  updatedAt = null;
}

function resetForSnapshot(reason) {
  clearBook();

  gapRecovery = false;
  gapDetected = false;

  lastResetReason = reason || "unknown";

  resyncs++;

  console.log(
    `[SYNC] Reset for snapshot. Reason=${lastResetReason}`
  );
}

// ============================================================
// SNAPSHOT INSTALLATION
// ============================================================

function installSnapshot(snapshot) {
  const snapshotId = Number(snapshot.lastUpdateId);

  if (!Number.isFinite(snapshotId)) {
    console.log("[SNAPSHOT] Invalid snapshot update ID");
    return false;
  }

  bids.clear();
  asks.clear();

  for (const level of snapshot.bids || []) {
    const price = toNumber(level[0]);
    const qty = toNumber(level[1]);

    if (price > 0 && qty > 0) {
      bids.set(price, qty);
    }
  }

  for (const level of snapshot.asks || []) {
    const price = toNumber(level[0]);
    const qty = toNumber(level[1]);

    if (price > 0 && qty > 0) {
      asks.set(price, qty);
    }
  }

  lastUpdateId = snapshotId;
  lastSnapshotId = snapshotId;

  initialized = false;
  waitingForBridge = true;

  gapRecovery = false;
  gapDetected = false;

  updatedAt = nowIso();

  console.log(
    `[SNAPSHOT] Installed. ID=${snapshotId} ` +
    `bids=${bids.size} asks=${asks.size}`
  );

  return true;
}

// ============================================================
// APPLY DEPTH EVENT
// ============================================================

function applyDepthEvent(event) {
  if (!event) return false;

  const U = Number(event.U);
  const u = Number(event.u);

  if (!Number.isFinite(U) || !Number.isFinite(u)) {
    return false;
  }

  if (u <= lastUpdateId) {
    return true;
  }

  for (const level of event.b || []) {
    const price = toNumber(level[0]);
    const qty = toNumber(level[1]);

    if (!(price > 0)) continue;

    if (qty === 0) {
      bids.delete(price);
    } else {
      bids.set(price, qty);
    }
  }

  for (const level of event.a || []) {
    const price = toNumber(level[0]);
    const qty = toNumber(level[1]);

    if (!(price > 0)) continue;

    if (qty === 0) {
      asks.delete(price);
    } else {
      asks.set(price, qty);
    }
  }

  lastUpdateId = u;

  updatedAt = nowIso();

  return true;
}

// ============================================================
// BRIDGE SEARCH
// ============================================================

function findBridgeEvent(snapshotId) {
  const target = snapshotId + 1;

  for (const event of pendingDepthEvents) {
    const U = Number(event.U);
    const u = Number(event.u);

    if (!Number.isFinite(U) || !Number.isFinite(u)) {
      continue;
    }

    if (U <= target && u >= target) {
      return event;
    }
  }

  return null;
}

// ============================================================
// COMPLETE SNAPSHOT BRIDGE
// ============================================================

function tryCompleteBridge() {
  if (!waitingForBridge) return false;

  if (!lastSnapshotId) return false;

  bridgeAttempts++;

  const bridge =
    findBridgeEvent(lastSnapshotId);

  if (!bridge) {
    return false;
  }

  console.log(
    `[SYNC] Bridge found. ` +
    `Snapshot=${lastSnapshotId} ` +
    `Event=${bridge.U}-${bridge.u}`
  );

  bridgeFound++;

  applyDepthEvent(bridge);

  initialized = true;
  waitingForBridge = false;

  gapRecovery = false;
  gapDetected = false;

  pendingDepthEvents = [];

  updatedAt = nowIso();

  console.log(
    `[SYNC] LIVE. ` +
    `updateId=${lastUpdateId} ` +
    `bids=${bids.size} ` +
    `asks=${asks.size}`
  );

  return true;
}

// ============================================================
// GAP RECOVERY
// ============================================================

function enterGapRecovery(event) {
  gapRecovery = true;
  gapDetected = true;

  gapCount++;

  lastGapLocal = lastUpdateId;
  lastGapIncoming = Number(event.U);

  console.log(
    `[SYNC] GAP detected. ` +
    `local=${lastUpdateId} ` +
    `incoming=${event.U}-${event.u}`
  );

  if (
    pendingDepthEvents.length <
    MAX_PENDING_EVENTS
  ) {
    pendingDepthEvents.push(event);
  }

  scheduleSnapshotRecovery();
}

// ============================================================
// SNAPSHOT RECOVERY TIMER
// ============================================================

function scheduleSnapshotRecovery() {
  if (snapshotPending) return;

  if (snapshotTimer) return;

  const now = Date.now();

  let waitMs = 0;

  if (lastSnapshotRequest) {
    const elapsed =
      now -
      new Date(
        lastSnapshotRequest
      ).getTime();

    waitMs =
      Math.max(
        0,
        SNAPSHOT_MIN_INTERVAL_MS -
          elapsed
      );
  }

  snapshotTimer = setTimeout(() => {
    snapshotTimer = null;

    requestSnapshot("gap-recovery");
  }, waitMs);

  console.log(
    `[SNAPSHOT] Recovery scheduled in ${waitMs}ms`
  );
}

// ============================================================
// HANDLE DEPTH EVENT
// ============================================================

function handleDepthEvent(event) {
  if (!event) return;

  const U = Number(event.U);
  const u = Number(event.u);

  if (!Number.isFinite(U) || !Number.isFinite(u)) {
    return;
  }

  // ----------------------------------------------------------
  // Waiting for snapshot bridge
  // ----------------------------------------------------------

  if (waitingForBridge) {
    if (
      pendingDepthEvents.length <
      MAX_PENDING_EVENTS
    ) {
      pendingDepthEvents.push(event);
    }

    tryCompleteBridge();

    return;
  }

  // ----------------------------------------------------------
  // Gap recovery
  // ----------------------------------------------------------

  if (gapRecovery) {
    if (
      pendingDepthEvents.length <
      MAX_PENDING_EVENTS
    ) {
      pendingDepthEvents.push(event);
    }

    // If the stream eventually gives us an event that
    // overlaps the next required update, recover immediately.
    if (
      U <= lastUpdateId + 1 &&
      u >= lastUpdateId + 1
    ) {
      applyDepthEvent(event);

      gapRecovery = false;
      gapDetected = false;

      pendingDepthEvents = [];

      console.log(
        `[SYNC] Gap recovered from live stream. ` +
        `updateId=${lastUpdateId}`
      );
    }

    return;
  }

  // ----------------------------------------------------------
  // Not initialized
  // ----------------------------------------------------------

  if (!initialized) {
    if (
      pendingDepthEvents.length <
      MAX_PENDING_EVENTS
    ) {
      pendingDepthEvents.push(event);
    }

    tryCompleteBridge();

    return;
  }

  // ----------------------------------------------------------
  // Old event
  // ----------------------------------------------------------

  if (u <= lastUpdateId) {
    return;
  }

  // ----------------------------------------------------------
  // Normal contiguous event
  // ----------------------------------------------------------

  if (
    U <= lastUpdateId + 1 &&
    u >= lastUpdateId + 1
  ) {
    applyDepthEvent(event);
    return;
  }

  // ----------------------------------------------------------
  // Genuine gap
  // ----------------------------------------------------------

  if (U > lastUpdateId + 1) {
    enterGapRecovery(event);
    return;
  }
}

// ============================================================
// DEPTH WEBSOCKET
// ============================================================

function connectDepthWebSocket() {
  clearReconnectTimer();

  console.log(
    `[WS] Connecting depth stream: ${DEPTH_WS}`
  );

  safeClose(depthWs);

  depthWs =
    new WebSocket(DEPTH_WS);

  depthWs.on("open", () => {
    console.log(
      "[WS] Depth WebSocket connected"
    );

    if (
      !initialized &&
      !waitingForBridge &&
      !snapshotPending
    ) {
      requestSnapshot("startup");
    }
  });

  depthWs.on("message", raw => {
    try {
      const event =
        JSON.parse(
          raw.toString()
        );

      if (
        event &&
        event.e === "depthUpdate" &&
        event.s === SYMBOL
      ) {
        handleDepthEvent(event);
      }
    } catch (err) {
      console.log(
        "[WS] Depth message parse error:",
        err.message
      );
    }
  });

  depthWs.on("close", () => {
    console.log(
      "[WS] Depth WebSocket closed"
    );

    scheduleDepthReconnect();
  });

  depthWs.on("error", err => {
    console.log(
      "[WS] Depth WebSocket error:",
      err.message
    );
  });
}

function scheduleDepthReconnect() {
  if (reconnectTimer) return;

  reconnectTimer =
    setTimeout(() => {
      reconnectTimer = null;

      connectDepthWebSocket();
    }, 5000);
}

// ============================================================
// SNAPSHOT WEBSOCKET
// ============================================================

function connectSnapshotWebSocket() {
  safeClose(snapshotWs);

  console.log(
    `[WS] Connecting snapshot API: ${WS_API}`
  );

  snapshotWs =
    new WebSocket(WS_API);

  snapshotWs.on("open", () => {
    console.log(
      "[WS] Snapshot WebSocket API connected"
    );
  });

  snapshotWs.on("message", raw => {
    try {
      const message =
        JSON.parse(
          raw.toString()
        );

      if (
        message &&
        message.id &&
        message.status
      ) {
        handleSnapshotResponse(message);
      }
    } catch (err) {
      console.log(
        "[WS] Snapshot message parse error:",
        err.message
      );
    }
  });

  snapshotWs.on("close", () => {
    console.log(
      "[WS] Snapshot WebSocket API closed"
    );

    setTimeout(() => {
      connectSnapshotWebSocket();
    }, 5000);
  });

  snapshotWs.on("error", err => {
    console.log(
      "[WS] Snapshot WebSocket error:",
      err.message
    );
  });
}

// ============================================================
// SNAPSHOT REQUEST
// ============================================================

function requestSnapshot(reason) {
  if (snapshotPending) {
    return false;
  }

  if (
    initialized &&
    !gapRecovery
  ) {
    return false;
  }

  const now = Date.now();

  if (lastSnapshotRequest) {
    const elapsed =
      now -
      new Date(
        lastSnapshotRequest
      ).getTime();

    if (
      elapsed <
      SNAPSHOT_MIN_INTERVAL_MS
    ) {
      const remaining =
        SNAPSHOT_MIN_INTERVAL_MS -
        elapsed;

      console.log(
        `[SNAPSHOT] Cooldown active. ` +
        `Waiting ${remaining}ms`
      );

      if (!snapshotTimer) {
        snapshotTimer =
          setTimeout(() => {
            snapshotTimer = null;

            requestSnapshot(reason);
          }, remaining);
      }

      return false;
    }
  }

  if (
    !snapshotWs ||
    snapshotWs.readyState !==
      WebSocket.OPEN
  ) {
    console.log(
      "[SNAPSHOT] Snapshot WebSocket not connected"
    );

    return false;
  }

  snapshotPending = true;

  snapshotRequests++;

  lastSnapshotRequest =
    nowIso();

  console.log(
    `[SNAPSHOT] Requesting depth snapshot. ` +
    `Reason=${reason}`
  );

  const requestId =
    `${Date.now()}-${Math.floor(
      Math.random() * 100000
    )}`;

  const request = {
    id: requestId,

    method: "depth",

    params: {
      symbol: SYMBOL,
      limit: 1000
    }
  };

  try {
    snapshotWs.send(
      JSON.stringify(request)
    );
  } catch (err) {
    snapshotPending = false;

    lastSnapshotError =
      err.message;

    console.log(
      "[SNAPSHOT] Send error:",
      err.message
    );

    return false;
  }

  setTimeout(() => {
    if (!snapshotPending) {
      return;
    }

    snapshotPending = false;

    lastSnapshotError =
      "Snapshot request timed out";

    console.log(
      "[SNAPSHOT] Request timeout"
    );

    scheduleSnapshotRecovery();
  }, SNAPSHOT_TIMEOUT_MS);

  return true;
}

// ============================================================
// SNAPSHOT RESPONSE
// ============================================================

function handleSnapshotResponse(message) {
  if (message.status !== 200) {
    snapshotPending = false;

    lastSnapshotError =
      message.error
        ? JSON.stringify(
            message.error
          )
        : `status ${message.status}`;

    if (
      message.error &&
      Number(
        message.error.code
      ) === -1003
    ) {
      snapshot429s++;

      console.log(
        "[SNAPSHOT] Binance rate limit 429"
      );
    } else {
      console.log(
        "[SNAPSHOT] Error:",
        lastSnapshotError
      );
    }

    scheduleSnapshotRecovery();

    return;
  }

  if (!message.result) {
    snapshotPending = false;

    lastSnapshotError =
      "Snapshot response missing result";

    scheduleSnapshotRecovery();

    return;
  }

  snapshotPending = false;

  lastSnapshotResponse =
    nowIso();

  lastSnapshotError = null;

  const installed =
    installSnapshot(
      message.result
    );

  if (!installed) {
    scheduleSnapshotRecovery();
    return;
  }

  tryCompleteBridge();

  if (!initialized) {
    console.log(
      "[SNAPSHOT] Waiting for bridge event..."
    );
  }
}

// ============================================================
// CURRENT PRICE
// ============================================================

function calculateCurrentPrice() {
  let bestBid = 0;
  let bestAsk = 0;

  for (const [price, qty] of bids) {
    if (
      qty > 0 &&
      price > bestBid
    ) {
      bestBid = price;
    }
  }

  for (const [price, qty] of asks) {
    if (
      qty > 0 &&
      (
        bestAsk === 0 ||
        price < bestAsk
      )
    ) {
      bestAsk = price;
    }
  }

  if (
    bestBid > 0 &&
    bestAsk > 0
  ) {
    return (
      bestBid +
      bestAsk
    ) / 2;
  }

  if (bestBid > 0) {
    return bestBid;
  }

  if (bestAsk > 0) {
    return bestAsk;
  }

  return null;
}

// ============================================================
// LEVEL EXTRACTION
// ============================================================

function getLevels(
  map,
  side,
  referencePrice
) {
  const output = [];

  const maxDistance =
    referencePrice *
    RANGE_PCT;

  for (
    const [price, qty]
    of map
  ) {
    if (!(qty > 0)) {
      continue;
    }

    const usd =
      price * qty;

    if (!(usd > 0)) {
      continue;
    }

    const distance =
      Math.abs(
        price -
        referencePrice
      );

    if (
      distance >
      maxDistance
    ) {
      continue;
    }

    if (
      side === "bid" &&
      price >= referencePrice
    ) {
      continue;
    }

    if (
      side === "ask" &&
      price <= referencePrice
    ) {
      continue;
    }

    output.push({
      price,
      qty,
      usd
    });
  }

  return output;
}

// ============================================================
// LOCALIZED CONCENTRATION CLUSTERS
// ============================================================
//
// NEW CLUSTER ENGINE
//
// Instead of:
//   "neighboring levels = same cluster"
//
// We now:
//   1. Find strong liquidity seeds.
//   2. Build a small zone around each seed.
//   3. Require enough meaningful levels.
//   4. Require enough total USD.
//   5. Remove overlapping areas.
//   6. Find the next independent concentration.
//
// This prevents the entire order book from becoming one cluster.
// ============================================================

function buildClusters(
  levels,
  side,
  referencePrice
) {
  if (
    !referencePrice ||
    !levels.length
  ) {
    return [];
  }

  // Only meaningful levels can start a cluster.
  const seedCandidates =
    levels
      .filter(level =>
        level.usd >=
        SEED_LEVEL_USD
      )
      .sort(
        (a, b) =>
          b.usd -
          a.usd
      );

  if (!seedCandidates.length) {
    return [];
  }

  const clusters = [];

  // Prices already consumed by stronger clusters.
  const usedPrices = [];

  for (
    const seed
    of seedCandidates
  ) {
    if (
      clusters.length >=
      MAX_CLUSTERS
    ) {
      break;
    }

    // --------------------------------------------------------
    // Do not create another cluster too close to an existing
    // stronger concentration.
    // --------------------------------------------------------

    let tooClose = false;

    for (
      const existing
      of clusters
    ) {
      const distance =
        Math.abs(
          seed.price -
          existing.strongestPrice
        );

      if (
        distance <
        MIN_CLUSTER_SEPARATION_USD
      ) {
        tooClose = true;
        break;
      }
    }

    if (tooClose) {
      continue;
    }

    // --------------------------------------------------------
    // Collect nearby meaningful levels.
    // --------------------------------------------------------

    const nearby =
      levels.filter(level => {
        const distance =
          Math.abs(
            level.price -
            seed.price
          );

        if (
          distance >
          CLUSTER_RADIUS_USD
        ) {
          return false;
        }

        if (
          level.usd <
          MIN_CLUSTER_LEVEL_USD
        ) {
          return false;
        }

        // Do not allow one level to belong to two
        // strong clusters.
        if (
          usedPrices.some(
            price =>
              price ===
              level.price
          )
        ) {
          return false;
        }

        return true;
      });

    if (!nearby.length) {
      continue;
    }

    // --------------------------------------------------------
    // Calculate concentration.
    // --------------------------------------------------------

    let totalUsd = 0;
    let totalQuantity = 0;

    let strongestPrice =
      seed.price;

    let strongestUsd =
      seed.usd;

    for (
      const level
      of nearby
    ) {
      totalUsd +=
        level.usd;

      totalQuantity +=
        level.qty;

      if (
        level.usd >
        strongestUsd
      ) {
        strongestUsd =
          level.usd;

        strongestPrice =
          level.price;
      }
    }

    // --------------------------------------------------------
    // Need enough total liquidity.
    // --------------------------------------------------------

    if (
      totalUsd <
      MIN_CLUSTER_USD
    ) {
      continue;
    }

    // --------------------------------------------------------
    // Need at least several meaningful levels.
    // --------------------------------------------------------

    if (
      nearby.length <
      MIN_CLUSTER_LEVELS
    ) {
      continue;
    }

    // --------------------------------------------------------
    // Determine actual range.
    // --------------------------------------------------------

    let priceLow =
      nearby[0].price;

    let priceHigh =
      nearby[0].price;

    for (
      const level
      of nearby
    ) {
      priceLow =
        Math.min(
          priceLow,
          level.price
        );

      priceHigh =
        Math.max(
          priceHigh,
          level.price
        );
    }

    const midpoint =
      (
        priceLow +
        priceHigh
      ) / 2;

    const distancePct =
      (
        (
          midpoint -
          referencePrice
        ) /
        referencePrice
      ) * 100;

    const cluster = {
      side,

      priceLow:
        Number(
          priceLow.toFixed(2)
        ),

      priceHigh:
        Number(
          priceHigh.toFixed(2)
        ),

      midpoint:
        Number(
          midpoint.toFixed(2)
        ),

      strongestPrice:
        Number(
          strongestPrice.toFixed(2)
        ),

      strongestUsd:
        Number(
          strongestUsd.toFixed(2)
        ),

      totalUsd:
        Number(
          totalUsd.toFixed(2)
        ),

      totalQuantity:
        Number(
          totalQuantity.toFixed(6)
        ),

      levels:
        nearby.length,

      distancePct:
        Number(
          distancePct.toFixed(3)
        )
    };

    clusters.push(
      cluster
    );

    // Mark these prices as consumed.
    for (
      const level
      of nearby
    ) {
      usedPrices.push(
        level.price
      );
    }
  }

  // Strongest concentration first.
  clusters.sort(
    (a, b) =>
      b.totalUsd -
      a.totalUsd
  );

  return clusters;
}

// ============================================================
// LIQUIDITY MAP
// ============================================================

function buildLiquidity() {
  const price =
    currentPrice ||
    calculateCurrentPrice();

  if (!price) {
    return {
      symbol: SYMBOL,

      currentPrice: null,

      settings: {
        minClusterUsd:
          MIN_CLUSTER_USD,

        rangePct:
          RANGE_PCT,

        clusterRadiusUsd:
          CLUSTER_RADIUS_USD,

        minClusterLevels:
          MIN_CLUSTER_LEVELS,

        seedLevelUsd:
          SEED_LEVEL_USD,

        minClusterLevelUsd:
          MIN_CLUSTER_LEVEL_USD,

        minClusterSeparationUsd:
          MIN_CLUSTER_SEPARATION_USD
      },

      buyLiquidity: [],

      sellLiquidity: [],

      summary: {
        buyClusters: 0,
        sellClusters: 0,
        totalBuyUsd: 0,
        totalSellUsd: 0
      },

      book: {
        initialized,
        bidLevels:
          bids.size,
        askLevels:
          asks.size,
        lastUpdateId
      },

      sync: {
        status:
          initialized
            ? (
                gapRecovery
                  ? "gap_recovery"
                  : "live"
              )
            : (
                waitingForBridge
                  ? "waiting_for_bridge"
                  : "syncing"
              ),

        gapRecovery,

        gapDetected,

        gapCount,

        lastGapLocal,

        lastGapIncoming,

        pendingEvents:
          pendingDepthEvents.length,

        snapshotPending,

        snapshotId:
          lastSnapshotId
      },

      connections: {
        depthWebSocket:
          depthWs &&
          depthWs.readyState ===
            WebSocket.OPEN,

        snapshotWebSocket:
          snapshotWs &&
          snapshotWs.readyState ===
            WebSocket.OPEN
      },

      updatedAt
    };
  }

  currentPrice = price;

  const bidLevels =
    getLevels(
      bids,
      "bid",
      price
    );

  const askLevels =
    getLevels(
      asks,
      "ask",
      price
    );

  const buyLiquidity =
    buildClusters(
      bidLevels,
      "bid",
      price
    );

  const sellLiquidity =
    buildClusters(
      askLevels,
      "ask",
      price
    );

  const totalBuyUsd =
    buyLiquidity.reduce(
      (sum, cluster) =>
        sum +
        cluster.totalUsd,
      0
    );

  const totalSellUsd =
    sellLiquidity.reduce(
      (sum, cluster) =>
        sum +
        cluster.totalUsd,
      0
    );

  return {
    symbol: SYMBOL,

    currentPrice:
      Number(
        price.toFixed(2)
      ),

    settings: {
      minClusterUsd:
        MIN_CLUSTER_USD,

      rangePct:
        RANGE_PCT,

      clusterRadiusUsd:
        CLUSTER_RADIUS_USD,

      minClusterLevels:
        MIN_CLUSTER_LEVELS,

      seedLevelUsd:
        SEED_LEVEL_USD,

      minClusterLevelUsd:
        MIN_CLUSTER_LEVEL_USD,

      minClusterSeparationUsd:
        MIN_CLUSTER_SEPARATION_USD
    },

    buyLiquidity,

    sellLiquidity,

    summary: {
      buyClusters:
        buyLiquidity.length,

      sellClusters:
        sellLiquidity.length,

      totalBuyUsd:
        Number(
          totalBuyUsd.toFixed(2)
        ),

      totalSellUsd:
        Number(
          totalSellUsd.toFixed(2)
        )
    },

    book: {
      initialized,

      bidLevels:
        bids.size,

      askLevels:
        asks.size,

      lastUpdateId
    },

    sync: {
      status:
        initialized
          ? (
              gapRecovery
                ? "gap_recovery"
                : "live"
            )
          : (
              waitingForBridge
                ? "waiting_for_bridge"
                : "syncing"
            ),

      gapRecovery,

      gapDetected,

      gapCount,

      lastGapLocal,

      lastGapIncoming,

      pendingEvents:
        pendingDepthEvents.length,

      snapshotPending,

      snapshotId:
        lastSnapshotId
    },

    connections: {
      depthWebSocket:
        depthWs &&
        depthWs.readyState ===
          WebSocket.OPEN,

      snapshotWebSocket:
        snapshotWs &&
        snapshotWs.readyState ===
          WebSocket.OPEN
    },

    updatedAt
  };
}

// ============================================================
// HEALTH
// ============================================================

app.get(
  "/health",
  (req, res) => {
    const depthConnected =
      depthWs &&
      depthWs.readyState ===
        WebSocket.OPEN;

    const snapshotConnected =
      snapshotWs &&
      snapshotWs.readyState ===
        WebSocket.OPEN;

    let status =
      "syncing";

    if (
      initialized &&
      !gapRecovery
    ) {
      status = "live";
    } else if (
      initialized &&
      gapRecovery
    ) {
      status =
        "gap_recovery";
    } else if (
      waitingForBridge
    ) {
      status =
        "waiting_for_bridge";
    }

    res.json({
      ok: true,

      symbol: SYMBOL,

      status,

      initialized,

      waitingForBridge,

      snapshotPending,

      gapRecovery,

      gapDetected,

      depthConnected,

      snapshotConnected,

      currentPrice,

      bidLevels:
        bids.size,

      askLevels:
        asks.size,

      pendingEvents:
        pendingDepthEvents.length,

      lastUpdateId,

      resyncs,

      snapshotRequests,

      snapshot429s,

      bridgeAttempts,

      bridgeFound,

      gapCount,

      lastGapLocal,

      lastGapIncoming,

      lastSnapshotRequest,

      lastSnapshotResponse,

      lastSnapshotError,

      lastResetReason,

      updatedAt
    });
  }
);

// ============================================================
// LIQUIDITY ENDPOINT
// ============================================================

app.get(
  "/liquidity",
  (req, res) => {
    try {
      res.json(
        buildLiquidity()
      );
    } catch (err) {
      res.status(500).json({
        ok: false,
        error: err.message
      });
    }
  }
);

// ============================================================
// FULL BOOK ENDPOINT
// ============================================================

app.get(
  "/book",
  (req, res) => {
    const price =
      currentPrice ||
      calculateCurrentPrice();

    const bidsArray =
      Array.from(
        bids.entries()
      )
        .map(
          ([price, qty]) => ({
            price,
            qty,
            usd:
              price * qty
          })
        )
        .sort(
          (a, b) =>
            b.price -
            a.price
        );

    const asksArray =
      Array.from(
        asks.entries()
      )
        .map(
          ([price, qty]) => ({
            price,
            qty,
            usd:
              price * qty
          })
        )
        .sort(
          (a, b) =>
            a.price -
            b.price
        );

    res.json({
      symbol: SYMBOL,

      currentPrice:
        price,

      initialized,

      gapRecovery,

      lastUpdateId,

      bids:
        bidsArray,

      asks:
        asksArray,

      bidLevels:
        bids.size,

      askLevels:
        asks.size,

      updatedAt
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
      ok: true,

      service:
        "Binance Futures Liquidity Relay",

      symbol:
        SYMBOL,

      endpoints: {
        health:
          "/health",

        liquidity:
          "/liquidity",

        book:
          "/book"
      }
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
      `Server listening on port ${PORT}`
    );

    console.log(
      `[CONFIG] Symbol=${SYMBOL}`
    );

    console.log(
      `[CONFIG] Range=${RANGE_PCT * 100}%`
    );

    console.log(
      `[CONFIG] Seed level=$${SEED_LEVEL_USD}`
    );

    console.log(
      `[CONFIG] Minimum cluster=$${MIN_CLUSTER_USD}`
    );

    console.log(
      `[CONFIG] Cluster radius=$${CLUSTER_RADIUS_USD}`
    );

    console.log(
      `[CONFIG] Cluster separation=$${MIN_CLUSTER_SEPARATION_USD}`
    );

    connectSnapshotWebSocket();

    connectDepthWebSocket();
  }
);
