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

const SNAPSHOT_MIN_INTERVAL_MS = 65000;

const SNAPSHOT_TIMEOUT_MS = 15000;

const PRICE_BUCKET_SIZE = 1;

const MIN_CLUSTER_USD = 500000;
const MIN_CLUSTER_LEVELS = 3;

const SEED_LEVEL_USD = 100000;

const MAX_EMPTY_BUCKETS = 1;

const RANGE_PCT = 0.03;

const MAX_PENDING_EVENTS = 20000;

const MAX_CLUSTERS = 12;

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

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
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
    `[SYNC] Reset for snapshot. Reason: ${lastResetReason}`
  );
}

// ============================================================
// APPLY SNAPSHOT
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
    `[SNAPSHOT] Installed. ID=${snapshotId} bids=${bids.size} asks=${asks.size}`
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

  // Ignore events already covered by our local book.
  if (u <= lastUpdateId) {
    return true;
  }

  // Apply bids.
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

  // Apply asks.
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

    // Event must span snapshotId + 1.
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

  const bridge = findBridgeEvent(lastSnapshotId);

  if (!bridge) {
    return false;
  }

  console.log(
    `[SYNC] Bridge found. Snapshot=${lastSnapshotId} ` +
    `Event=${bridge.U}-${bridge.u}`
  );

  bridgeFound++;

  // Apply ONLY the bridge event.
  applyDepthEvent(bridge);

  initialized = true;
  waitingForBridge = false;

  gapRecovery = false;
  gapDetected = false;

  // IMPORTANT:
  // Do NOT replay the entire old buffer.
  //
  // The old implementation replayed every buffered event after
  // the bridge. If one old event appeared to have a gap, it wiped
  // the valid book.
  //
  // We now start clean from the bridge point.
  pendingDepthEvents = [];

  updatedAt = nowIso();

  console.log(
    `[SYNC] LIVE. updateId=${lastUpdateId} ` +
    `bids=${bids.size} asks=${asks.size}`
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

  // IMPORTANT:
  // Do NOT clear the existing book.
  //
  // The old code immediately reset everything here.
  // That caused:
  //
  // bidLevels: 0
  // askLevels: 0
  //
  // while waiting for another snapshot.
  //
  // We keep the current book alive while recovery happens.

  if (pendingDepthEvents.length < MAX_PENDING_EVENTS) {
    pendingDepthEvents.push(event);
  }

  scheduleSnapshotRecovery();
}

// ============================================================
// SCHEDULE SNAPSHOT RECOVERY
// ============================================================

function scheduleSnapshotRecovery() {
  if (snapshotPending) return;

  if (snapshotTimer) return;

  const now = Date.now();

  let waitMs = 0;

  if (lastSnapshotRequest) {
    const elapsed =
      now - new Date(lastSnapshotRequest).getTime();

    waitMs = Math.max(
      0,
      SNAPSHOT_MIN_INTERVAL_MS - elapsed
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
  // Always keep a small history while waiting for bridge.
  // ----------------------------------------------------------

  if (waitingForBridge) {
    if (pendingDepthEvents.length < MAX_PENDING_EVENTS) {
      pendingDepthEvents.push(event);
    }

    tryCompleteBridge();

    return;
  }

  // ----------------------------------------------------------
  // If we are in gap recovery:
  //
  // Keep the existing book.
  // If a later event overlaps the missing update, we can
  // continue without waiting for another snapshot.
  // ----------------------------------------------------------

  if (gapRecovery) {
    if (pendingDepthEvents.length < MAX_PENDING_EVENTS) {
      pendingDepthEvents.push(event);
    }

    // If the incoming event bridges our local next update,
    // apply it and recover.
    if (U <= lastUpdateId + 1 && u >= lastUpdateId + 1) {
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
  // Normal live synchronization.
  // ----------------------------------------------------------

  if (!initialized) {
    if (pendingDepthEvents.length < MAX_PENDING_EVENTS) {
      pendingDepthEvents.push(event);
    }

    tryCompleteBridge();

    return;
  }

  // Event is completely old.
  if (u <= lastUpdateId) {
    return;
  }

  // Normal contiguous / overlapping event.
  if (U <= lastUpdateId + 1 && u >= lastUpdateId + 1) {
    applyDepthEvent(event);
    return;
  }

  // Genuine gap.
  if (U > lastUpdateId + 1) {
    enterGapRecovery(event);
    return;
  }

  // Otherwise ignore.
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

  depthWs = new WebSocket(DEPTH_WS);

  depthWs.on("open", () => {
    console.log("[WS] Depth WebSocket connected");

    // Do not immediately request snapshots repeatedly.
    // Initial snapshot is requested only once.
    if (!initialized && !waitingForBridge && !snapshotPending) {
      requestSnapshot("startup");
    }
  });

  depthWs.on("message", raw => {
    try {
      const event = JSON.parse(raw.toString());

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
    console.log("[WS] Depth WebSocket closed");

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

  reconnectTimer = setTimeout(() => {
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

  snapshotWs = new WebSocket(WS_API);

  snapshotWs.on("open", () => {
    console.log(
      "[WS] Snapshot WebSocket API connected"
    );
  });

  snapshotWs.on("message", raw => {
    try {
      const message = JSON.parse(raw.toString());

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

  // If already live and not recovering, no snapshot needed.
  if (initialized && !gapRecovery) {
    return false;
  }

  const now = Date.now();

  if (lastSnapshotRequest) {
    const elapsed =
      now - new Date(lastSnapshotRequest).getTime();

    if (elapsed < SNAPSHOT_MIN_INTERVAL_MS) {
      const remaining =
        SNAPSHOT_MIN_INTERVAL_MS - elapsed;

      console.log(
        `[SNAPSHOT] Rate-limit cooldown active. ` +
        `Waiting ${remaining}ms`
      );

      if (!snapshotTimer) {
        snapshotTimer = setTimeout(() => {
          snapshotTimer = null;

          requestSnapshot(reason);
        }, remaining);
      }

      return false;
    }
  }

  if (
    !snapshotWs ||
    snapshotWs.readyState !== WebSocket.OPEN
  ) {
    console.log(
      "[SNAPSHOT] Snapshot WebSocket not connected"
    );

    return false;
  }

  snapshotPending = true;

  snapshotRequests++;

  lastSnapshotRequest = nowIso();

  console.log(
    `[SNAPSHOT] Requesting depth snapshot. Reason=${reason}`
  );

  const requestId =
    `${Date.now()}-${Math.floor(Math.random() * 100000)}`;

  const request = {
    id: requestId,
    method: "depth",
    params: {
      symbol: SYMBOL,
      limit: 1000
    }
  };

  try {
    snapshotWs.send(JSON.stringify(request));
  } catch (err) {
    snapshotPending = false;

    lastSnapshotError = err.message;

    console.log(
      "[SNAPSHOT] Send error:",
      err.message
    );

    return false;
  }

  setTimeout(() => {
    if (!snapshotPending) return;

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
        ? JSON.stringify(message.error)
        : `status ${message.status}`;

    if (
      message.error &&
      Number(message.error.code) === -1003
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

  lastSnapshotResponse = nowIso();

  lastSnapshotError = null;

  const installed =
    installSnapshot(message.result);

  if (!installed) {
    scheduleSnapshotRecovery();
    return;
  }

  // Immediately attempt the bridge.
  tryCompleteBridge();

  // If the bridge is not in the current buffer yet,
  // leave the snapshot installed and wait for more events.
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
    if (qty > 0 && price > bestBid) {
      bestBid = price;
    }
  }

  for (const [price, qty] of asks) {
    if (
      qty > 0 &&
      (bestAsk === 0 || price < bestAsk)
    ) {
      bestAsk = price;
    }
  }

  if (bestBid > 0 && bestAsk > 0) {
    return (bestBid + bestAsk) / 2;
  }

  if (bestBid > 0) return bestBid;

  if (bestAsk > 0) return bestAsk;

  return null;
}

// ============================================================
// ORDER BOOK LEVELS
// ============================================================

function getLevels(map, side, price) {
  const output = [];

  for (const [levelPrice, qty] of map) {
    if (!(qty > 0)) continue;

    const usd = levelPrice * qty;

    if (!(usd > 0)) continue;

    if (side === "bid" && levelPrice >= price) {
      continue;
    }

    if (side === "ask" && levelPrice <= price) {
      continue;
    }

    output.push({
      price: levelPrice,
      qty,
      usd
    });
  }

  output.sort((a, b) => {
    return side === "bid"
      ? b.price - a.price
      : a.price - b.price;
  });

  return output;
}

// ============================================================
// CLUSTERING
// ============================================================

function buildClusters(levels, side, referencePrice) {
  if (!referencePrice || !levels.length) {
    return [];
  }

  const maxDistance =
    referencePrice * RANGE_PCT;

  const filtered = levels.filter(level => {
    const distance =
      Math.abs(level.price - referencePrice);

    return distance <= maxDistance;
  });

  if (!filtered.length) {
    return [];
  }

  const buckets = new Map();

  for (const level of filtered) {
    const bucket =
      Math.round(
        level.price / PRICE_BUCKET_SIZE
      ) * PRICE_BUCKET_SIZE;

    if (!buckets.has(bucket)) {
      buckets.set(bucket, {
        bucket,
        usd: 0,
        quantity: 0,
        levels: 0,
        strongestPrice: level.price,
        strongestUsd: level.usd
      });
    }

    const b = buckets.get(bucket);

    b.usd += level.usd;
    b.quantity += level.qty;
    b.levels++;

    if (level.usd > b.strongestUsd) {
      b.strongestUsd = level.usd;
      b.strongestPrice = level.price;
    }
  }

  const sortedBuckets =
    Array.from(buckets.values()).sort((a, b) => {
      return a.bucket - b.bucket;
    });

  const clusters = [];

  let current = null;

  for (const bucket of sortedBuckets) {
    if (!current) {
      current = {
        priceLow: bucket.bucket,
        priceHigh: bucket.bucket,
        totalUsd: bucket.usd,
        totalQuantity: bucket.quantity,
        levels: bucket.levels,
        strongestPrice: bucket.strongestPrice,
        strongestUsd: bucket.strongestUsd,
        emptyBuckets: 0
      };

      continue;
    }

    const distance =
      Math.abs(
        bucket.bucket -
        current.priceHigh
      );

    const expectedGap =
      PRICE_BUCKET_SIZE;

    const emptyCount =
      Math.max(
        0,
        Math.round(
          distance / expectedGap
        ) - 1
      );

    if (emptyCount <= MAX_EMPTY_BUCKETS) {
      current.priceHigh =
        Math.max(
          current.priceHigh,
          bucket.bucket
        );

      current.priceLow =
        Math.min(
          current.priceLow,
          bucket.bucket
        );

      current.totalUsd += bucket.usd;

      current.totalQuantity +=
        bucket.quantity;

      current.levels += bucket.levels;

      current.emptyBuckets +=
        emptyCount;

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
      clusters.push(current);

      current = {
        priceLow: bucket.bucket,
        priceHigh: bucket.bucket,
        totalUsd: bucket.usd,
        totalQuantity: bucket.quantity,
        levels: bucket.levels,
        strongestPrice: bucket.strongestPrice,
        strongestUsd: bucket.strongestUsd,
        emptyBuckets: 0
      };
    }
  }

  if (current) {
    clusters.push(current);
  }

  // Require enough concentration.
  const qualified =
    clusters.filter(cluster => {
      return (
        cluster.totalUsd >= MIN_CLUSTER_USD &&
        cluster.levels >= MIN_CLUSTER_LEVELS
      );
    });

  // Sort by total USD.
  qualified.sort((a, b) => {
    return b.totalUsd - a.totalUsd;
  });

  const finalClusters =
    qualified.slice(0, MAX_CLUSTERS);

  return finalClusters.map(cluster => {
    const midpoint =
      (
        cluster.priceLow +
        cluster.priceHigh
      ) / 2;

    const distancePct =
      (
        (midpoint - referencePrice) /
        referencePrice
      ) * 100;

    return {
      side,

      priceLow:
        Number(cluster.priceLow.toFixed(2)),

      priceHigh:
        Number(cluster.priceHigh.toFixed(2)),

      midpoint:
        Number(midpoint.toFixed(2)),

      strongestPrice:
        Number(
          cluster.strongestPrice.toFixed(2)
        ),

      strongestUsd:
        Number(
          cluster.strongestUsd.toFixed(2)
        ),

      totalUsd:
        Number(
          cluster.totalUsd.toFixed(2)
        ),

      totalQuantity:
        Number(
          cluster.totalQuantity.toFixed(6)
        ),

      levels:
        cluster.levels,

      distancePct:
        Number(
          distancePct.toFixed(3)
        )
    };
  });
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
        minClusterUsd: MIN_CLUSTER_USD,
        rangePct: RANGE_PCT,
        priceBucketSize: PRICE_BUCKET_SIZE,
        minClusterLevels: MIN_CLUSTER_LEVELS
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
        bidLevels: bids.size,
        askLevels: asks.size,
        lastUpdateId
      },

      sync: {
        gapRecovery,
        gapDetected,
        gapCount,
        lastGapLocal,
        lastGapIncoming
      },

      connections: {
        depthWebSocket:
          depthWs &&
          depthWs.readyState === WebSocket.OPEN,

        snapshotWebSocket:
          snapshotWs &&
          snapshotWs.readyState === WebSocket.OPEN
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
        sum + cluster.totalUsd,
      0
    );

  const totalSellUsd =
    sellLiquidity.reduce(
      (sum, cluster) =>
        sum + cluster.totalUsd,
      0
    );

  return {
    symbol: SYMBOL,

    currentPrice:
      Number(price.toFixed(2)),

    settings: {
      minClusterUsd:
        MIN_CLUSTER_USD,

      rangePct:
        RANGE_PCT,

      priceBucketSize:
        PRICE_BUCKET_SIZE,

      minClusterLevels:
        MIN_CLUSTER_LEVELS,

      seedLevelUsd:
        SEED_LEVEL_USD,

      maxEmptyBuckets:
        MAX_EMPTY_BUCKETS
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
        depthWs.readyState === WebSocket.OPEN,

      snapshotWebSocket:
        snapshotWs &&
        snapshotWs.readyState === WebSocket.OPEN
    },

    updatedAt
  };
}

// ============================================================
// HEALTH
// ============================================================

app.get("/health", (req, res) => {
  const depthConnected =
    depthWs &&
    depthWs.readyState === WebSocket.OPEN;

  const snapshotConnected =
    snapshotWs &&
    snapshotWs.readyState === WebSocket.OPEN;

  let status = "syncing";

  if (initialized && !gapRecovery) {
    status = "live";
  } else if (initialized && gapRecovery) {
    status = "gap_recovery";
  } else if (waitingForBridge) {
    status = "waiting_for_bridge";
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

    currentPrice:
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
});

// ============================================================
// LIQUIDITY ENDPOINT
// ============================================================

app.get("/liquidity", (req, res) => {
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
});

// ============================================================
// BOOK ENDPOINT
// ============================================================

app.get("/book", (req, res) => {
  const price =
    currentPrice ||
    calculateCurrentPrice();

  const bidsArray =
    Array.from(bids.entries())
      .map(([price, qty]) => ({
        price,
        qty,
        usd:
          price * qty
      }))
      .sort(
        (a, b) =>
          b.price - a.price
      );

  const asksArray =
    Array.from(asks.entries())
      .map(([price, qty]) => ({
        price,
        qty,
        usd:
          price * qty
      }))
      .sort(
        (a, b) =>
          a.price - b.price
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
});

// ============================================================
// ROOT
// ============================================================

app.get("/", (req, res) => {
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
});

// ============================================================
// START SERVER
// ============================================================

app.listen(PORT, () => {
  console.log(
    `Server listening on port ${PORT}`
  );

  console.log(
    `[CONFIG] Symbol=${SYMBOL}`
  );

  console.log(
    `[CONFIG] Snapshot cooldown=${SNAPSHOT_MIN_INTERVAL_MS}ms`
  );

  console.log(
    `[CONFIG] Cluster minimum=$${MIN_CLUSTER_USD}`
  );

  console.log(
    `[CONFIG] Range=${RANGE_PCT * 100}%`
  );

  connectSnapshotWebSocket();

  connectDepthWebSocket();
});
