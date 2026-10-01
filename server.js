/* ============================================================
   BTCUSDT LIQUIDITY RELAY V12
   Binance Futures Order Book + Spot Historical Structure
   Render + NxCreate Telegram compatible

   MAJOR FIXES
   ------------------------------------------------------------
   1. Correct Binance diff-depth snapshot synchronization
   2. Snapshot generation protection
   3. Old buffered events cannot bridge a new snapshot
   4. Bridge event is applied exactly once
   5. Overlapping depth events handled correctly
   6. Genuine sequence gaps only trigger resync
   7. No repeated false resync loop
   8. Stable structural cache
   9. Directionally correct BSL / SSL
  10. Concentrated order-book liquidity
  11. Persistent liquidity
  12. Structural confluence
  13. Closed-candle liquidity sweep detection
============================================================ */

const express = require("express");
const WebSocket = require("ws");

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 10000;
const SYMBOL = "BTCUSDT";

const SPOT_API = "https://data-api.binance.vision";

const DEPTH_WS =
  "wss://fstream.binance.com/ws/btcusdt@depth@100ms";

const KLINE_WS =
  "wss://fstream.binance.com/stream?streams=" +
  "btcusdt@kline_15m/" +
  "btcusdt@kline_1h/" +
  "btcusdt@kline_4h/" +
  "btcusdt@kline_1d";

const WS_API =
  "wss://ws-fapi.binance.com/ws-fapi/v1";

/* ============================================================
   SETTINGS
============================================================ */

const SETTINGS = {
  rawMinUsd: 50000,

  restingRadiusUsd: 35,
  restingConfirmUsd: 250000,

  mergeUsd: 35,
  zonePct: 0.0015,
  reactionPct: 0.003,

  minScore: 40,
  maxLevels: 8,

  /* ORDER BOOK */
  bookBucketUsd: 5,
  orderBucketMinUsd: 250000,
  orderConcentrationMultiple: 2.5,

  orderMinUsd: 750000,
  orderSingleBucketMinUsd: 1000000,

  minStrongBuckets: 2,
  orderMaxClusterWidthUsd: 100,
  orderSplitRatio: 0.45,

  /* PERSISTENCE */
  persistencePollMs: 5000,
  persistenceConfirmMs: 60000,
  persistenceMaxAgeMs: 10 * 60 * 1000,

  persistenceTolerancePct: 0.0005,
  persistenceRemovedMinObservations: 2,
  persistenceRemovedMinLifetimeMs: 10000,

  eventMax: 100,

  /* SWEEPS */
  sweepLookbackMs: 24 * 60 * 60 * 1000,
  sweepMinPenetrationPct: 0.00015,

  /* MAJOR STRUCTURE */
  majorMinDistancePct: 0.50,
  majorMinimumTimeframeWeight: 2,
  major1hMinScore: 90,
  majorHigherTfMinScore: 40,

  /* STRUCTURE CACHE */
  structureCacheMaxAgeMs: 30 * 60 * 1000,

  /* CONFLUENCE */
  confluenceMaxDistancePct: 0.001,
  confluenceMaxDistanceUsd: 75,
  confluenceMinOverlapRatio: 0.20
};

const STRUCTURE = {
  "15m": {
    interval: "15m",
    weight: 1,
    candles: 300,
    left: 3,
    right: 3,
    eq: 0.0015
  },

  "1h": {
    interval: "1h",
    weight: 2,
    candles: 300,
    left: 4,
    right: 4,
    eq: 0.0015
  },

  "4h": {
    interval: "4h",
    weight: 3,
    candles: 250,
    left: 5,
    right: 5,
    eq: 0.0020
  },

  "1d": {
    interval: "1d",
    weight: 4,
    candles: 180,
    left: 5,
    right: 5,
    eq: 0.0025
  }
};

/* ============================================================
   GLOBAL STATE
============================================================ */

const bids = new Map();
const asks = new Map();

let currentPrice = null;

const candles = {
  "15m": [],
  "1h": [],
  "4h": [],
  "1d": []
};

/* ============================================================
   DEPTH SYNCHRONIZATION STATE
============================================================ */

let depthSocket = null;
let snapshotSocket = null;
let snapshotRequestId = 0;

let initialized = false;
let waitingForBridge = true;
let snapshotPending = false;
let snapshotInFlight = false;

let lastUpdateId = 0;

let pendingEvents = [];

let syncGeneration = 0;
let resyncScheduled = false;

let lastSnapshotId = 0;
let lastBridgeU = 0;

let lastGapExpected = 0;
let lastGapReceived = 0;

let lastAppliedEventU = 0;
let lastAppliedEventUStart = 0;

let snapshotRequests = 0;
let snapshot429s = 0;
let bridgeAttempts = 0;
let bridgeFound = 0;

let resyncs = 0;
let sequenceGaps = 0;

let depthConnected = false;
let snapshotConnected = false;
let klineConnected = false;
let websocketConnected = false;

let lastError = null;

let startedAt = new Date().toISOString();

/* ============================================================
   STRUCTURE STATE
============================================================ */

const structure = {
  swingHighs: [],
  swingLows: [],

  equalHighs: [],
  equalLows: [],

  levels: [],
  majorLevels: [],
  nearLevels: [],

  lastGoodRebuildAt: null,
  lastAttemptAt: null,

  rebuildCount: 0,
  rejectedRebuilds: 0,

  status: "INITIALIZING",

  lastRebuildAccepted: false,
  lastRebuildReason: null,

  lastCandidateLevels: 0,
  lastCandidateMajor: 0,

  lastGoodLevels: [],
  lastGoodMajorLevels: [],

  lastGoodAt: null,

  retainedPrevious: false
};

/* ============================================================
   SWEEP / EVENTS
============================================================ */

const recentSweeps = [];
const liquidityEvents = [];

/* ============================================================
   PERSISTENCE
============================================================ */

const persistenceStore = new Map();

let lastPersistencePoll = 0;

/* ============================================================
   UTILITIES
============================================================ */

function now() {
  return Date.now();
}

function iso(ms = Date.now()) {
  return new Date(ms).toISOString();
}

function safeNumber(v, fallback = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

function clamp(v, min, max) {
  return Math.max(min, Math.min(max, v));
}

function pctDistance(price, reference) {
  if (!reference) return 0;
  return ((price - reference) / reference) * 100;
}

function absPct(price, reference) {
  if (!reference) return 0;
  return Math.abs((price - reference) / reference);
}

function clearMap(map) {
  map.clear();
}

/* ============================================================
   BINANCE SNAPSHOT / DEPTH
============================================================ */

/*
   IMPORTANT:

   Binance diff-depth synchronization:

   1. Start websocket.
   2. Buffer events.
   3. Request snapshot.
   4. Snapshot gives S.
   5. Find first buffered event where:

        U <= S + 1
        u >= S + 1

   6. Apply snapshot.
   7. Apply bridge event exactly once.
   8. lastUpdateId = bridge.u
   9. Apply later events.
  10. Ignore events where u <= lastUpdateId.
  11. Accept event if:

        U <= lastUpdateId + 1
        AND
        u >= lastUpdateId + 1

  12. Gap only when:

        U > lastUpdateId + 1
*/

function resetSyncForNewSnapshot() {
  waitingForBridge = true;
  snapshotPending = true;
  snapshotInFlight = false;

  /*
     CRITICAL FIX:

     Events buffered before the new snapshot are NOT safe
     to apply after a new snapshot unless they satisfy the
     bridge condition against that exact snapshot.

     We keep only a bounded recent buffer.
  */
  if (pendingEvents.length > 2000) {
    pendingEvents = pendingEvents.slice(-1000);
  }
}

function applyDepthEvent(event) {
  if (!event || !event.b || !event.a) return;

  for (const row of event.b) {
    const price = safeNumber(row[0]);
    const qty = safeNumber(row[1]);

    if (!price) continue;

    if (qty <= 0) {
      bids.delete(price);
    } else {
      bids.set(price, qty);
    }
  }

  for (const row of event.a) {
    const price = safeNumber(row[0]);
    const qty = safeNumber(row[1]);

    if (!price) continue;

    if (qty <= 0) {
      asks.delete(price);
    } else {
      asks.set(price, qty);
    }
  }

  lastAppliedEventUStart = safeNumber(event.U);
  lastAppliedEventU = safeNumber(event.u);

  if (bids.size && asks.size) {
    const bestBid = Math.max(...bids.keys());
    const bestAsk = Math.min(...asks.keys());

    if (
      Number.isFinite(bestBid) &&
      Number.isFinite(bestAsk) &&
      bestBid > 0 &&
      bestAsk > 0 &&
      bestAsk >= bestBid
    ) {
      currentPrice = (bestBid + bestAsk) / 2;
    }
  }
}

function isBridgeEvent(event, snapshotId) {
  const U = safeNumber(event.U);
  const u = safeNumber(event.u);

  if (!U || !u || !snapshotId) return false;

  return U <= snapshotId + 1 && u >= snapshotId + 1;
}

function isApplicableEvent(event, previousId) {
  const U = safeNumber(event.U);
  const u = safeNumber(event.u);

  if (!U || !u) return false;

  /*
     Already completely processed.
  */
  if (u <= previousId) {
    return false;
  }

  /*
     Normal Binance overlapping event.
  */
  return U <= previousId + 1 && u >= previousId + 1;
}

function isGapEvent(event, previousId) {
  const U = safeNumber(event.U);
  const u = safeNumber(event.u);

  if (!U || !u) return false;

  if (u <= previousId) return false;

  return U > previousId + 1;
}

function processPostSnapshotEvents(events, generation) {
  if (generation !== syncGeneration) return false;

  let cursor = lastUpdateId;

  for (const event of events) {
    if (generation !== syncGeneration) {
      return false;
    }

    const u = safeNumber(event.u);

    if (!u) continue;

    /*
       Old/duplicate event.
    */
    if (u <= cursor) {
      continue;
    }

    /*
       Correct overlapping event.
    */
    if (isApplicableEvent(event, cursor)) {
      applyDepthEvent(event);
      cursor = lastUpdateId;
      continue;
    }

    /*
       Actual missing sequence.
    */
    if (isGapEvent(event, cursor)) {
      lastGapExpected = cursor + 1;
      lastGapReceived = safeNumber(event.U);

      sequenceGaps++;

      scheduleResync("post_snapshot_sequence_gap");

      return false;
    }
  }

  lastUpdateId = cursor;

  return true;
}

async function requestDepthSnapshot() {
  if (snapshotInFlight) return;

  const generation = ++syncGeneration;

  snapshotInFlight = true;
  snapshotPending = true;
  waitingForBridge = true;

  snapshotRequests++;

  /*
     CRITICAL:

     Do not let an old snapshot response compete with a new
     synchronization generation.
  */

  try {
    if (!snapshotSocket || snapshotSocket.readyState !== WebSocket.OPEN) {
      openSnapshotSocket();
    }

    const id = ++snapshotRequestId;

    const payload = {
      id: String(id),
      method: "depth",
      params: {
        symbol: SYMBOL,
        limit: 1000
      }
    };

    const result = await new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        reject(new Error("Snapshot timeout"));
      }, 10000);

      const handler = (raw) => {
        let msg;

        try {
          msg = JSON.parse(raw.toString());
        } catch {
          return;
        }

        if (String(msg.id) !== String(id)) {
          return;
        }

        clearTimeout(timeout);

        snapshotSocket.off("message", handler);

        resolve(msg);
      };

      snapshotSocket.on("message", handler);

      try {
        snapshotSocket.send(JSON.stringify(payload));
      } catch (err) {
        clearTimeout(timeout);
        snapshotSocket.off("message", handler);
        reject(err);
      }
    });

    /*
       This snapshot is stale if another sync cycle started.
    */
    if (generation !== syncGeneration) {
      return;
    }

    if (result.code) {
      if (result.code === 429) {
        snapshot429s++;
      }

      throw new Error(
        `Snapshot error ${result.code}: ${result.msg || "unknown"}`
      );
    }

    const data = result.result || result;

    const snapshotId = safeNumber(data.lastUpdateId);

    if (!snapshotId) {
      throw new Error("Snapshot missing lastUpdateId");
    }

    /*
       IMPORTANT FIX:

       Search ONLY events that were received before/during
       this exact snapshot generation.

       The bridge must be:

       U <= S+1
       u >= S+1
    */
    const bridgeIndex = pendingEvents.findIndex((event) =>
      isBridgeEvent(event, snapshotId)
    );

    bridgeAttempts++;

    if (bridgeIndex === -1) {
      /*
         Snapshot is newer than our currently buffered events.

         DO NOT apply old events.

         Keep only events received after the snapshot request
         generation can reasonably use. Since events are
         continuously arriving, retry with a fresh snapshot.
      */

      lastSnapshotId = snapshotId;

      /*
         Remove events that can never bridge this snapshot.
      */
      pendingEvents = pendingEvents.filter(
        (event) => safeNumber(event.u) > snapshotId
      );

      snapshotInFlight = false;

      setTimeout(() => {
        if (generation === syncGeneration) {
          requestDepthSnapshot().catch(handleError);
        }
      }, 300);

      return;
    }

    bridgeFound++;

    const bridgeEvent = pendingEvents[bridgeIndex];

    /*
       Anything before bridge cannot be applied.
    */
    const afterBridge = pendingEvents.slice(bridgeIndex + 1);

    /*
       Initialize book from snapshot.
    */
    clearMap(bids);
    clearMap(asks);

    if (Array.isArray(data.bids)) {
      for (const row of data.bids) {
        const price = safeNumber(row[0]);
        const qty = safeNumber(row[1]);

        if (price > 0 && qty > 0) {
          bids.set(price, qty);
        }
      }
    }

    if (Array.isArray(data.asks)) {
      for (const row of data.asks) {
        const price = safeNumber(row[0]);
        const qty = safeNumber(row[1]);

        if (price > 0 && qty > 0) {
          asks.set(price, qty);
        }
      }
    }

    lastSnapshotId = snapshotId;

    /*
       APPLY BRIDGE EXACTLY ONCE.
    */
    applyDepthEvent(bridgeEvent);

    lastUpdateId = safeNumber(bridgeEvent.u);
    lastBridgeU = lastUpdateId;

    /*
       Remove all events through bridge.
    */
    pendingEvents = afterBridge;

    /*
       Apply subsequent buffered events.
    */
    const successful = processPostSnapshotEvents(
      afterBridge,
      generation
    );

    if (!successful) {
      return;
    }

    /*
       Sync is now valid.
    */
    initialized = true;
    waitingForBridge = false;
    snapshotPending = false;
    snapshotInFlight = false;

    resyncScheduled = false;

    lastError = null;

    websocketConnected = depthConnected && snapshotConnected;

  } catch (err) {
    if (generation === syncGeneration) {
      snapshotInFlight = false;
      lastError = err.message || String(err);

      setTimeout(() => {
        if (generation === syncGeneration) {
          requestDepthSnapshot().catch(handleError);
        }
      }, 1000);
    }
  }
}

function scheduleResync(reason = "sequence_gap") {
  if (resyncScheduled) return;

  resyncScheduled = true;

  /*
     Do NOT immediately destroy the current book.

     It remains usable as a temporary display while the new
     snapshot is acquired.
  */

  const wasInitialized = initialized;

  if (wasInitialized) {
    resyncs++;
  }

  initialized = false;
  waitingForBridge = true;
  snapshotPending = true;

  /*
     New generation invalidates all previous snapshot work.
  */
  syncGeneration++;

  pendingEvents = [];

  setTimeout(() => {
    resyncScheduled = false;

    requestDepthSnapshot().catch(handleError);
  }, 500);
}

function handleDepthMessage(raw) {
  let event;

  try {
    event = JSON.parse(raw.toString());
  } catch {
    return;
  }

  if (
    !event ||
    event.e !== "depthUpdate" ||
    !event.U ||
    !event.u
  ) {
    return;
  }

  /*
     Always buffer incoming events until the current snapshot
     synchronization is fully established.
  */
  pendingEvents.push(event);

  if (pendingEvents.length > 3000) {
    pendingEvents = pendingEvents.slice(-2000);
  }

  /*
     Before initialization we DO NOT process the event directly.
  */
  if (!initialized) {
    if (
      !snapshotInFlight &&
      !resyncScheduled
    ) {
      requestDepthSnapshot().catch(handleError);
    }

    return;
  }

  /*
     Already initialized.

     Remove events that are already covered.
  */
  if (safeNumber(event.u) <= lastUpdateId) {
    pendingEvents = pendingEvents.filter(
      (x) => safeNumber(x.u) > lastUpdateId
    );

    return;
  }

  /*
     Correct overlapping event.
  */
  if (isApplicableEvent(event, lastUpdateId)) {
    applyDepthEvent(event);

    pendingEvents = pendingEvents.filter(
      (x) => safeNumber(x.u) > lastUpdateId
    );

    return;
  }

  /*
     Actual gap.
  */
  if (isGapEvent(event, lastUpdateId)) {
    lastGapExpected = lastUpdateId + 1;
    lastGapReceived = safeNumber(event.U);

    sequenceGaps++;

    scheduleResync("live_sequence_gap");
  }
}

/* ============================================================
   DEPTH WEBSOCKET
============================================================ */

function connectDepth() {
  if (depthSocket) {
    try {
      depthSocket.close();
    } catch {}
  }

  depthSocket = new WebSocket(DEPTH_WS);

  depthSocket.on("open", () => {
    depthConnected = true;
    websocketConnected = true;

    pendingEvents = [];

    initialized = false;
    waitingForBridge = true;

    requestDepthSnapshot().catch(handleError);
  });

  depthSocket.on("message", handleDepthMessage);

  depthSocket.on("close", () => {
    depthConnected = false;
    websocketConnected = false;

    setTimeout(connectDepth, 2000);
  });

  depthSocket.on("error", (err) => {
    lastError = err.message || String(err);
  });
}

/* ============================================================
   SNAPSHOT WEBSOCKET
============================================================ */

function openSnapshotSocket() {
  if (
    snapshotSocket &&
    snapshotSocket.readyState === WebSocket.OPEN
  ) {
    return;
  }

  try {
    snapshotSocket = new WebSocket(WS_API);

    snapshotSocket.on("open", () => {
      snapshotConnected = true;
    });

    snapshotSocket.on("close", () => {
      snapshotConnected = false;

      setTimeout(openSnapshotSocket, 2000);
    });

    snapshotSocket.on("error", (err) => {
      snapshotConnected = false;
      lastError = err.message || String(err);
    });

  } catch (err) {
    snapshotConnected = false;
    lastError = err.message || String(err);
  }
}

/* ============================================================
   SPOT HISTORICAL CANDLES
============================================================ */

async function fetchSpotKlines(interval, limit) {
  const url =
    `${SPOT_API}/api/v3/klines` +
    `?symbol=${SYMBOL}` +
    `&interval=${interval}` +
    `&limit=${limit}`;

  const response = await fetch(url);

  if (!response.ok) {
    throw new Error(
      `Spot klines ${interval} HTTP ${response.status}`
    );
  }

  const data = await response.json();

  return data.map((x) => ({
    openTime: safeNumber(x[0]),
    open: safeNumber(x[1]),
    high: safeNumber(x[2]),
    low: safeNumber(x[3]),
    close: safeNumber(x[4]),
    volume: safeNumber(x[5])
  }));
}

async function loadHistoricalCandles() {
  for (const tf of Object.keys(STRUCTURE)) {
    try {
      const cfg = STRUCTURE[tf];

      candles[tf] = await fetchSpotKlines(
        cfg.interval,
        cfg.candles
      );
    } catch (err) {
      lastError = err.message || String(err);
    }
  }
}

/* ============================================================
   LIVE KLINES
============================================================ */

function upsertCandle(tf, candle, closed) {
  if (!candles[tf]) {
    candles[tf] = [];
  }

  const arr = candles[tf];

  const existingIndex = arr.findIndex(
    (x) => x.openTime === candle.openTime
  );

  if (existingIndex >= 0) {
    arr[existingIndex] = candle;
  } else {
    arr.push(candle);
    arr.sort((a, b) => a.openTime - b.openTime);

    if (arr.length > 500) {
      arr.splice(0, arr.length - 500);
    }
  }

  if (closed) {
    detectSweepsForTimeframe(tf, candle);

    rebuildStructure("closed_candle");
  }
}

function connectKlines() {
  const ws = new WebSocket(KLINE_WS);

  ws.on("open", () => {
    klineConnected = true;
  });

  ws.on("message", (raw) => {
    let msg;

    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }

    const k = msg?.data?.k;

    if (!k) return;

    const interval = k.i;

    let tf = null;

    if (interval === "15m") tf = "15m";
    if (interval === "1h") tf = "1h";
    if (interval === "4h") tf = "4h";
    if (interval === "1d") tf = "1d";

    if (!tf) return;

    const candle = {
      openTime: safeNumber(k.t),
      open: safeNumber(k.o),
      high: safeNumber(k.h),
      low: safeNumber(k.l),
      close: safeNumber(k.c),
      volume: safeNumber(k.v)
    };

    currentPrice = candle.close;

    upsertCandle(tf, candle, Boolean(k.x));
  });

  ws.on("close", () => {
    klineConnected = false;

    setTimeout(connectKlines, 2000);
  });

  ws.on("error", (err) => {
    lastError = err.message || String(err);
  });
}

/* ============================================================
   PIVOTS
============================================================ */

function detectPivots(arr, left, right) {
  const highs = [];
  const lows = [];

  if (arr.length < left + right + 1) {
    return { highs, lows };
  }

  for (
    let i = left;
    i < arr.length - right;
    i++
  ) {
    const h = arr[i].high;
    const l = arr[i].low;

    let highPivot = true;
    let lowPivot = true;

    for (let j = i - left; j <= i + right; j++) {
      if (j === i) continue;

      if (arr[j].high >= h) {
        highPivot = false;
      }

      if (arr[j].low <= l) {
        lowPivot = false;
      }
    }

    if (highPivot) {
      highs.push({
        price: h,
        time: arr[i].openTime
      });
    }

    if (lowPivot) {
      lows.push({
        price: l,
        time: arr[i].openTime
      });
    }
  }

  return { highs, lows };
}

/* ============================================================
   EQUAL LEVELS
============================================================ */

function clusterEqualLevels(points, tolerancePct) {
  const sorted = [...points].sort(
    (a, b) => a.price - b.price
  );

  const groups = [];

  for (const p of sorted) {
    let found = null;

    for (const group of groups) {
      const mid =
        group.reduce((s, x) => s + x.price, 0) /
        group.length;

      if (
        Math.abs(p.price - mid) / mid <=
        tolerancePct
      ) {
        found = group;
        break;
      }
    }

    if (found) {
      found.push(p);
    } else {
      groups.push([p]);
    }
  }

  return groups.map((group) => {
    const price =
      group.reduce((s, x) => s + x.price, 0) /
      group.length;

    return {
      price,
      count: group.length,
      points: group
    };
  });
}

/* ============================================================
   REACTION COUNT
============================================================ */

function reactionCount(arr, price) {
  let reactions = 0;

  for (let i = 0; i < arr.length; i++) {
    const c = arr[i];

    const touched =
      c.high >= price * (1 - SETTINGS.reactionPct) &&
      c.low <= price * (1 + SETTINGS.reactionPct);

    if (!touched) continue;

    const next = arr.slice(
      i + 1,
      Math.min(arr.length, i + 9)
    );

    if (!next.length) continue;

    const bullishReaction =
      next.some(
        (x) =>
          x.high >=
          price * (1 + SETTINGS.reactionPct)
      );

    const bearishReaction =
      next.some(
        (x) =>
          x.low <=
          price * (1 - SETTINGS.reactionPct)
      );

    if (bullishReaction || bearishReaction) {
      reactions++;
    }
  }

  return reactions;
}

/* ============================================================
   BUILD TIMEFRAME STRUCTURE
============================================================ */

function buildTFStructure(tf) {
  const cfg = STRUCTURE[tf];
  const arr = candles[tf] || [];

  const pivots = detectPivots(
    arr,
    cfg.left,
    cfg.right
  );

  const equalHighs = clusterEqualLevels(
    pivots.highs,
    cfg.eq
  );

  const equalLows = clusterEqualLevels(
    pivots.lows,
    cfg.eq
  );

  const levels = [];

  for (const group of equalHighs) {
    const prices = group.points.map((x) => x.price);

    levels.push({
      side: "BSL",
      price: group.price,
      priceLow: Math.min(...prices),
      priceHigh: Math.max(...prices),
      equalCount: group.count,
      reactions: reactionCount(arr, group.price),
      timeframes: [tf],
      timeframeWeight: cfg.weight
    });
  }

  for (const group of equalLows) {
    const prices = group.points.map((x) => x.price);

    levels.push({
      side: "SSL",
      price: group.price,
      priceLow: Math.min(...prices),
      priceHigh: Math.max(...prices),
      equalCount: group.count,
      reactions: reactionCount(arr, group.price),
      timeframes: [tf],
      timeframeWeight: cfg.weight
    });
  }

  return {
    tf,
    weight: cfg.weight,
    swingHighs: pivots.highs,
    swingLows: pivots.lows,
    equalHighs,
    equalLows,
    levels
  };
}

/* ============================================================
   MERGE STRUCTURAL LEVELS
============================================================ */

function mergeLevels(rawLevels) {
  const sorted = [...rawLevels].sort(
    (a, b) => a.price - b.price
  );

  const merged = [];

  for (const level of sorted) {
    const existing = merged.find(
      (x) =>
        x.side === level.side &&
        Math.abs(x.price - level.price) <=
          Math.max(
            SETTINGS.mergeUsd,
            level.price * SETTINGS.zonePct
          )
    );

    if (!existing) {
      merged.push({
        side: level.side,
        price: level.price,
        priceLow: level.priceLow,
        priceHigh: level.priceHigh,
        equalCount: level.equalCount,
        reactions: level.reactions,
        timeframes: [...level.timeframes],
        timeframeWeight: level.timeframeWeight
      });

      continue;
    }

    const totalWeight =
      existing.timeframeWeight +
      level.timeframeWeight;

    existing.price =
      (
        existing.price * existing.timeframeWeight +
        level.price * level.timeframeWeight
      ) / totalWeight;

    existing.priceLow = Math.min(
      existing.priceLow,
      level.priceLow
    );

    existing.priceHigh = Math.max(
      existing.priceHigh,
      level.priceHigh
    );

    existing.equalCount += level.equalCount;

    existing.reactions += level.reactions;

    for (const tf of level.timeframes) {
      if (!existing.timeframes.includes(tf)) {
        existing.timeframes.push(tf);
      }
    }

    existing.timeframeWeight = totalWeight;
  }

  return merged;
}

/* ============================================================
   ORDER BOOK ROWS
============================================================ */

function getBookRows(side) {
  const map = side === "bid" ? bids : asks;

  const rows = [];

  for (const [price, qty] of map.entries()) {
    const usd = price * qty;

    if (usd < SETTINGS.rawMinUsd) {
      continue;
    }

    rows.push({
      price,
      quantity: qty,
      usd
    });
  }

  return rows;
}

/* ============================================================
   ORDER BOOK BUCKETING
============================================================ */

function buildBookBuckets(side) {
  const rows = getBookRows(side);

  const bucketSize = SETTINGS.bookBucketUsd;

  const buckets = new Map();

  for (const row of rows) {
    const key =
      Math.floor(row.price / bucketSize) *
      bucketSize;

    let bucket = buckets.get(key);

    if (!bucket) {
      bucket = {
        index: Math.round(key / bucketSize),
        priceLow: key,
        priceHigh: key + bucketSize,
        usd: 0,
        rawLevels: 0,
        strongestPrice: row.price,
        strongestUsd: row.usd,
        strongestRawOrderUsd: row.usd
      };

      buckets.set(key, bucket);
    }

    bucket.usd += row.usd;
    bucket.rawLevels++;

    if (row.usd > bucket.strongestUsd) {
      bucket.strongestUsd = row.usd;
      bucket.strongestPrice = row.price;
    }

    bucket.strongestRawOrderUsd = Math.max(
      bucket.strongestRawOrderUsd,
      row.usd
    );
  }

  return [...buckets.values()].sort(
    (a, b) => a.priceLow - b.priceLow
  );
}

function median(values) {
  if (!values.length) return 0;

  const sorted = [...values].sort(
    (a, b) => a - b
  );

  const mid = Math.floor(sorted.length / 2);

  if (sorted.length % 2) {
    return sorted[mid];
  }

  return (sorted[mid - 1] + sorted[mid]) / 2;
}

/* ============================================================
   ORDER BOOK CLUSTERS
============================================================ */

function buildBookClusters(side) {
  const buckets = buildBookBuckets(side);

  if (!buckets.length) return [];

  const medianUsd = median(
    buckets.map((x) => x.usd)
  );

  const concentrationThreshold = Math.max(
    SETTINGS.orderBucketMinUsd,
    medianUsd *
      SETTINGS.orderConcentrationMultiple
  );

  const strong = buckets.map((bucket) => ({
    ...bucket,
    strong:
      bucket.usd >= concentrationThreshold
  }));

  const runs = [];

  let current = [];

  for (const bucket of strong) {
    if (!bucket.strong) {
      if (current.length) {
        runs.push(current);
        current = [];
      }

      continue;
    }

    if (!current.length) {
      current = [bucket];
      continue;
    }

    const previous =
      current[current.length - 1];

    const gap =
      bucket.index - previous.index;

    const width =
      bucket.priceHigh -
      current[0].priceLow;

    const valley =
      bucket.usd <
      Math.max(
        previous.usd,
        bucket.usd
      ) * SETTINGS.orderSplitRatio;

    if (
      gap > 1 ||
      width > SETTINGS.orderMaxClusterWidthUsd ||
      valley
    ) {
      runs.push(current);
      current = [bucket];
    } else {
      current.push(bucket);
    }
  }

  if (current.length) {
    runs.push(current);
  }

  const clusters = [];

  for (const run of runs) {
    const strongUsd = run.reduce(
      (s, x) => s + x.usd,
      0
    );

    const priceLow =
      run[0].priceLow;

    const priceHigh =
      run[run.length - 1].priceHigh;

    const width =
      priceHigh - priceLow;

    /*
       Calculate concentration against ALL order-book
       buckets inside the same price span.

       This makes concentrationRatio meaningful.
    */
    const allInSpan = buckets.filter(
      (x) =>
        x.priceLow >= priceLow &&
        x.priceHigh <= priceHigh
    );

    const allUsd = allInSpan.reduce(
      (s, x) => s + x.usd,
      0
    );

    const concentrationRatio =
      allUsd > 0
        ? strongUsd / allUsd
        : 0;

    const strongest =
      run.reduce(
        (best, x) =>
          x.usd > best.usd ? x : best,
        run[0]
      );

    const isSingle =
      run.length === 1;

    const singleAccepted =
      isSingle &&
      strongest.usd >=
        SETTINGS.orderSingleBucketMinUsd;

    const multiAccepted =
      !isSingle &&
      run.length >=
        SETTINGS.minStrongBuckets &&
      strongUsd >= SETTINGS.orderMinUsd;

    if (
      !singleAccepted &&
      !multiAccepted
    ) {
      continue;
    }

    clusters.push({
      side,

      priceLow,
      priceHigh,

      midpoint:
        (priceLow + priceHigh) / 2,

      usd: strongUsd,

      levels: run.length,

      rawLevels: run.reduce(
        (s, x) => s + x.rawLevels,
        0
      ),

      strongBuckets: run.length,

      strongestPrice:
        strongest.strongestPrice,

      strongestBucketUsd:
        strongest.usd,

      strongestRawOrderUsd:
        strongest.strongestRawOrderUsd,

      medianBucketUsd: medianUsd,

      concentrationThreshold,

      concentrationRatio,

      width,

      giantWall:
        strongest.usd >=
        SETTINGS.orderSingleBucketMinUsd
    });
  }

  return clusters
    .sort((a, b) => b.usd - a.usd)
    .slice(0, 20);
}

/* ============================================================
   STRUCTURAL SCORE
============================================================ */

function calculateBaseScore(level) {
  const tfScore =
    level.timeframeWeight * 10;

  const equalScore =
    Math.min(level.equalCount, 30) * 5;

  const reactionScore =
    Math.min(level.reactions, 40) * 2;

  return tfScore +
    equalScore +
    reactionScore;
}

function strengthForScore(score) {
  if (score >= 125) return "HIGH";
  if (score >= 90) return "MEDIUM";
  return "LOW";
}

/* ============================================================
   RESTING LIQUIDITY ATTACHMENT
============================================================ */

function attachRestingLiquidity(levels) {
  const bidClusters =
    buildBookClusters("bid");

  const askClusters =
    buildBookClusters("ask");

  for (const level of levels) {
    const relevant =
      level.side === "BSL"
        ? askClusters
        : bidClusters;

    let restingUsd = 0;

    const matched = [];

    for (const cluster of relevant) {
      const distance =
        Math.abs(
          cluster.midpoint -
          level.price
        );

      if (
        distance <=
        SETTINGS.restingRadiusUsd
      ) {
        restingUsd += cluster.usd;
        matched.push(cluster);
      }
    }

    /*
       Raw diagnostic orders only.
       Do not dump the entire order book.
    */
    const map =
      level.side === "BSL"
        ? asks
        : bids;

    const raw = [];

    for (const [price, quantity] of map.entries()) {
      if (
        Math.abs(price - level.price) <=
        SETTINGS.restingRadiusUsd
      ) {
        raw.push({
          price,
          quantity,
          usd: price * quantity
        });
      }
    }

    raw.sort((a, b) => b.usd - a.usd);

    level.restingUsd = restingUsd;

    level.restingConfirmed =
      restingUsd >=
      SETTINGS.restingConfirmUsd;

    level.restingLevels =
      raw.slice(0, 10);

    /*
       Concentrated order-book liquidity contributes
       modestly to structure score.
    */
    level.scoreBase =
      calculateBaseScore(level);

    const restingBonus =
      clamp(
        restingUsd / 100000,
        0,
        25
      );

    level.score =
      level.scoreBase +
      restingBonus;

    level.strength =
      strengthForScore(level.score);
  }

  return levels;
}

/* ============================================================
   MAJOR LEVELS
============================================================ */

function timeframeWeight(level) {
  return level.timeframeWeight;
}

function isMajorLevel(level, price) {
  if (!price || !level.price) {
    return false;
  }

  const distance =
    absPct(level.price, price);

  if (
    distance <
    SETTINGS.majorMinDistancePct / 100
  ) {
    return false;
  }

  const weight =
    timeframeWeight(level);

  if (
    weight <
    SETTINGS.majorMinimumTimeframeWeight
  ) {
    return false;
  }

  /*
     15m only cannot become major.
  */
  if (
    weight === 1 &&
    level.timeframes.length === 1
  ) {
    return false;
  }

  /*
     1h-only requires stronger score.
  */
  if (
    level.timeframes.length === 1 &&
    level.timeframes[0] === "1h"
  ) {
    return level.score >=
      SETTINGS.major1hMinScore;
  }

  /*
     Higher timeframe structure can qualify
     with score >= 40.
  */
  if (
    level.timeframes.includes("4h") ||
    level.timeframes.includes("1d")
  ) {
    return level.score >=
      SETTINGS.majorHigherTfMinScore;
  }

  /*
     1h + 15m.
  */
  if (
    level.timeframes.includes("1h") &&
    level.timeframes.includes("15m")
  ) {
    return level.score >=
      SETTINGS.major1hMinScore;
  }

  return false;
}

/* ============================================================
   FILTER CACHED STRUCTURE
============================================================ */

function filterCachedDirectionalLevels(
  levels,
  price
) {
  if (!price) return [];

  return levels.filter((level) => {
    if (!level || !level.price) {
      return false;
    }

    /*
       BSL must be ABOVE current price.
       SSL must be BELOW current price.
    */
    if (
      level.side === "BSL" &&
      level.price <= price
    ) {
      return false;
    }

    if (
      level.side === "SSL" &&
      level.price >= price
    ) {
      return false;
    }

    /*
       Don't keep something absurdly far away
       forever.
    */
    const distance =
      absPct(level.price, price);

    return distance <= 0.25;
  });
}

/* ============================================================
   REBUILD STRUCTURE
============================================================ */

function rebuildStructure(reason = "manual") {
  const price =
    currentPrice ||
    getMidPrice();

  if (!price) return;

  structure.lastAttemptAt =
    iso();

  structure.rebuildCount++;

  const tfStructures = [];

  for (const tf of Object.keys(STRUCTURE)) {
    const result =
      buildTFStructure(tf);

    tfStructures.push(result);
  }

  const rawLevels =
    tfStructures.flatMap(
      (x) => x.levels
    );

  let merged =
    mergeLevels(rawLevels);

  merged =
    attachRestingLiquidity(merged);

  for (const level of merged) {
    level.scoreBase =
      calculateBaseScore(level);

    level.distancePct =
      pctDistance(
        level.price,
        price
      );
  }

  const candidates =
    merged.filter(
      (x) => x.score >= SETTINGS.minScore
    );

  const majorCandidates =
    candidates.filter(
      (x) => isMajorLevel(x, price)
    );

  structure.lastCandidateLevels =
    candidates.length;

  structure.lastCandidateMajor =
    majorCandidates.length;

  /*
     ----------------------------------------------------------
     VALID STRUCTURE
     ----------------------------------------------------------
  */

  if (majorCandidates.length > 0) {
    structure.levels =
      candidates;

    structure.majorLevels =
      majorCandidates;

    structure.nearLevels =
      candidates.filter(
        (x) =>
          absPct(x.price, price) <
          0.005
      );

    structure.lastGoodLevels =
      JSON.parse(
        JSON.stringify(candidates)
      );

    structure.lastGoodMajorLevels =
      JSON.parse(
        JSON.stringify(majorCandidates)
      );

    structure.lastGoodAt =
      iso();

    structure.lastGoodRebuildAt =
      iso();

    structure.lastRebuildAccepted =
      true;

    structure.lastRebuildReason =
      "accepted_with_major_structure";

    structure.status =
      "HEALTHY";

    structure.retainedPrevious =
      false;

    structure.rejectedRebuilds++;

    return;
  }

  /*
     ----------------------------------------------------------
     NO MAJOR CANDIDATE

     Keep previous valid structure if it is still fresh.
     ----------------------------------------------------------
  */

  const lastGoodTime =
    structure.lastGoodAt
      ? new Date(
          structure.lastGoodAt
        ).getTime()
      : 0;

  const age =
    now() - lastGoodTime;

  const cachedMajor =
    filterCachedDirectionalLevels(
      structure.lastGoodMajorLevels,
      price
    );

  const cachedLevels =
    filterCachedDirectionalLevels(
      structure.lastGoodLevels,
      price
    );

  if (
    cachedMajor.length > 0 &&
    age <= SETTINGS.structureCacheMaxAgeMs
  ) {
    structure.levels =
      cachedLevels;

    structure.majorLevels =
      cachedMajor;

    structure.nearLevels =
      cachedLevels.filter(
        (x) =>
          absPct(x.price, price) <
          0.005
      );

    structure.lastRebuildAccepted =
      false;

    structure.lastRebuildReason =
      "retained_previous_major_structure";

    structure.status =
      "STALE_FALLBACK";

    structure.retainedPrevious =
      true;

    structure.rejectedRebuilds++;

    return;
  }

  /*
     No valid previous structure.
  */

  structure.levels = [];
  structure.majorLevels = [];
  structure.nearLevels = [];

  structure.lastRebuildAccepted =
    false;

  structure.lastRebuildReason =
    "rejected_empty_directional_structure";

  structure.status =
    "NO_STRUCTURE";

  structure.retainedPrevious =
    false;

  structure.rejectedRebuilds++;
}

/* ============================================================
   PRICE
============================================================ */

function getMidPrice() {
  if (!bids.size || !asks.size) {
    return currentPrice;
  }

  const bestBid =
    Math.max(...bids.keys());

  const bestAsk =
    Math.min(...asks.keys());

  if (
    !Number.isFinite(bestBid) ||
    !Number.isFinite(bestAsk)
  ) {
    return currentPrice;
  }

  return (
    bestBid + bestAsk
  ) / 2;
}

/* ============================================================
   DIRECTIONAL STRUCTURE
============================================================ */

function getDirectionalMajor(side) {
  const price =
    currentPrice ||
    getMidPrice();

  if (!price) return [];

  return structure.majorLevels
    .filter((level) => {
      if (side === "BSL") {
        return level.side === "BSL" &&
          level.price > price;
      }

      return level.side === "SSL" &&
        level.price < price;
    })
    .sort((a, b) => {
      if (side === "BSL") {
        return a.price - b.price;
      }

      return b.price - a.price;
    });
}

function getDirectionalNear(side) {
  const price =
    currentPrice ||
    getMidPrice();

  if (!price) return [];

  return structure.levels
    .filter((level) => {
      if (
        absPct(level.price, price) >=
        0.005
      ) {
        return false;
      }

      if (side === "BSL") {
        return (
          level.side === "BSL" &&
          level.price > price
        );
      }

      return (
        level.side === "SSL" &&
        level.price < price
      );
    })
    .sort((a, b) => {
      if (side === "BSL") {
        return a.price - b.price;
      }

      return b.price - a.price;
    });
}

/* ============================================================
   CONFLUENCE
============================================================ */

function findStructuralConfluence(
  cluster
) {
  const majors =
    structure.majorLevels;

  let best = null;

  for (const level of majors) {
    const expectedSide =
      cluster.side === "bid"
        ? "SSL"
        : "BSL";

    if (level.side !== expectedSide) {
      continue;
    }

    const overlapLow =
      Math.max(
        cluster.priceLow,
        level.priceLow
      );

    const overlapHigh =
      Math.min(
        cluster.priceHigh,
        level.priceHigh
      );

    const overlap =
      Math.max(
        0,
        overlapHigh - overlapLow
      );

    const bookWidth =
      Math.max(
        1,
        cluster.priceHigh -
          cluster.priceLow
      );

    const overlapRatio =
      overlap / bookWidth;

    const strongestInside =
      cluster.strongestPrice >=
        level.priceLow &&
      cluster.strongestPrice <=
        level.priceHigh;

    const distance =
      Math.abs(
        cluster.midpoint -
        level.price
      );

    const allowed =
      Math.max(
        SETTINGS.confluenceMaxDistanceUsd,
        level.price *
          SETTINGS.confluenceMaxDistancePct
      );

    const qualifies =
      strongestInside ||
      overlapRatio >=
        SETTINGS.confluenceMinOverlapRatio ||
      distance <= allowed;

    if (!qualifies) continue;

    if (
      !best ||
      distance < best.distance
    ) {
      best = {
        level,
        overlap,
        overlapRatio,
        distance
      };
    }
  }

  return best;
}

function enrichClusterWithConfluence(
  cluster
) {
  const result =
    findStructuralConfluence(
      cluster
    );

  if (!result) {
    return {
      ...cluster,
      structuralConfluence: false,
      structuralLevel: null,
      structuralZoneLow: null,
      structuralZoneHigh: null,
      structuralTimeframes: [],
      structuralStrength: null,
      structuralScore: null,
      structuralOverlap: false,
      structuralOverlapRatio: 0,
      structuralDistance: null
    };
  }

  const level =
    result.level;

  return {
    ...cluster,

    structuralConfluence: true,

    structuralLevel:
      level.price,

    structuralZoneLow:
      level.priceLow,

    structuralZoneHigh:
      level.priceHigh,

    structuralTimeframes:
      level.timeframes,

    structuralStrength:
      level.strength,

    structuralScore:
      level.score,

    structuralOverlap:
      true,

    structuralOverlapRatio:
      result.overlapRatio,

    structuralDistance:
      result.distance,

    quality:
      Math.round(
        cluster.usd / 100000 +
        level.score * 0.3 +
        result.overlapRatio * 20
      )
  };
}

/* ============================================================
   SWEEP DETECTION
============================================================ */

function detectSweepForLevel(
  side,
  level,
  candle,
  tf
) {
  const penetration =
    level.price *
    SETTINGS.sweepMinPenetrationPct;

  let swept = false;

  let sweepPrice = null;

  if (side === "BSL") {
    swept =
      candle.high >
        level.priceHigh + penetration &&
      candle.close <
        level.priceHigh;

    sweepPrice = candle.high;
  }

  if (side === "SSL") {
    swept =
      candle.low <
        level.priceLow - penetration &&
      candle.close >
        level.priceLow;

    sweepPrice = candle.low;
  }

  if (!swept) return;

  const cutoff =
    now() -
    SETTINGS.sweepLookbackMs;

  if (candle.openTime < cutoff) {
    return;
  }

  const duplicate =
    recentSweeps.some(
      (x) =>
        x.side === side &&
        x.timeframe === tf &&
        x.level === level.price &&
        x.candleTime ===
          candle.openTime
    );

  if (duplicate) return;

  const item = {
    side,

    level: level.price,

    zoneLow:
      level.priceLow,

    zoneHigh:
      level.priceHigh,

    sweepPrice,

    timeframe: tf,

    candleTime:
      candle.openTime,

    status: "SWEPT",

    reclaimed: true,

    detectedAt: iso(),

    timeframes:
      level.timeframes
  };

  recentSweeps.unshift(item);

  if (recentSweeps.length > 50) {
    recentSweeps.pop();
  }
}

function detectSweepsForTimeframe(
  tf,
  candle
) {
  const levels =
    structure.majorLevels;

  for (const level of levels) {
    detectSweepForLevel(
      level.side,
      level,
      candle,
      tf
    );
  }
}

/* ============================================================
   PERSISTENCE ID
============================================================ */

function persistenceId(cluster) {
  return [
    cluster.side,
    Math.round(
      cluster.midpoint /
      SETTINGS.bookBucketUsd
    )
  ].join(":");
}

/* ============================================================
   PERSISTENCE MATCH
============================================================ */

function clustersMatch(a, b) {
  if (!a || !b) return false;

  if (a.side !== b.side) {
    return false;
  }

  const distance =
    Math.abs(
      a.midpoint -
      b.midpoint
    );

  const allowed =
    Math.max(
      50,
      (currentPrice || b.midpoint) *
        SETTINGS.persistenceTolerancePct
    );

  return distance <= allowed;
}

/* ============================================================
   PERSISTENCE STATE
============================================================ */

function persistenceState(
  previousUsd,
  currentUsd
) {
  if (!previousUsd) {
    return "NEW";
  }

  const change =
    (currentUsd - previousUsd) /
    previousUsd;

  if (change >= 0.10) {
    return "STRENGTHENING";
  }

  if (change <= -0.10) {
    return "WEAKENING";
  }

  return "HOLDING";
}

/* ============================================================
   LIQUIDITY EVENT
============================================================ */

function addLiquidityEvent(event) {
  liquidityEvents.unshift({
    ...event,
    at: iso()
  });

  if (
    liquidityEvents.length >
    SETTINGS.eventMax
  ) {
    liquidityEvents.pop();
  }
}

/* ============================================================
   PERSISTENCE POLL
============================================================ */

function updatePersistence() {
  const ts = now();

  if (
    ts - lastPersistencePoll <
    SETTINGS.persistencePollMs
  ) {
    return;
  }

  lastPersistencePoll = ts;

  let clusters = [
    ...buildBookClusters("bid"),
    ...buildBookClusters("ask")
  ];

  clusters =
    clusters.map(
      enrichClusterWithConfluence
    );

  const seen = new Set();

  for (const cluster of clusters) {
    let existing = null;

    /*
       First exact ID.
    */
    const id =
      persistenceId(cluster);

    existing =
      persistenceStore.get(id);

    /*
       If exact ID changed, find nearby item.
    */
    if (!existing) {
      for (const item of persistenceStore.values()) {
        if (
          item.removed ||
          !item.active
        ) {
          continue;
        }

        if (
          clustersMatch(
            item,
            cluster
          )
        ) {
          existing = item;
          break;
        }
      }
    }

    if (!existing) {
      existing = {
        id,

        side: cluster.side,

        priceLow:
          cluster.priceLow,

        priceHigh:
          cluster.priceHigh,

        midpoint:
          cluster.midpoint,

        strongestPrice:
          cluster.strongestPrice,

        strongestBucketUsd:
          cluster.strongestBucketUsd,

        usd:
          cluster.usd,

        currentUsd:
          cluster.usd,

        previousUsd: 0,

        maxUsd:
          cluster.usd,

        firstSeen:
          iso(),

        lastSeen:
          iso(),

        durationMs: 0,

        durationSec: 0,

        observations: 1,

        status: "NEW",

        removed: false,

        active: true,

        persistent: false,

        structuralConfluence:
          cluster.structuralConfluence,

        structuralLevel:
          cluster.structuralLevel,

        structuralTimeframes:
          cluster.structuralTimeframes,

        structuralStrength:
          cluster.structuralStrength
      };

      persistenceStore.set(
        id,
        existing
      );

      seen.add(id);

      continue;
    }

    seen.add(existing.id);

    const previousUsd =
      existing.currentUsd || 0;

    existing.previousUsd =
      previousUsd;

    existing.currentUsd =
      cluster.usd;

    existing.usd =
      cluster.usd;

    existing.changeUsd =
      cluster.usd -
      previousUsd;

    existing.priceLow =
      cluster.priceLow;

    existing.priceHigh =
      cluster.priceHigh;

    existing.midpoint =
      cluster.midpoint;

    existing.strongestPrice =
      cluster.strongestPrice;

    existing.strongestBucketUsd =
      cluster.strongestBucketUsd;

    existing.maxUsd =
      Math.max(
        existing.maxUsd || 0,
        cluster.usd
      );

    existing.lastSeen =
      iso();

    existing.durationMs =
      ts -
      new Date(
        existing.firstSeen
      ).getTime();

    existing.durationSec =
      Math.round(
        existing.durationMs / 1000
      );

    existing.observations++;

    existing.status =
      persistenceState(
        previousUsd,
        cluster.usd
      );

    existing.removed = false;
    existing.active = true;

    existing.structuralConfluence =
      cluster.structuralConfluence;

    existing.structuralLevel =
      cluster.structuralLevel;

    existing.structuralTimeframes =
      cluster.structuralTimeframes;

    existing.structuralStrength =
      cluster.structuralStrength;

    const wasPersistent =
      existing.persistent;

    existing.persistent =
      existing.durationMs >=
      SETTINGS.persistenceConfirmMs;

    if (
      existing.persistent &&
      !wasPersistent
    ) {
      addLiquidityEvent({
        type:
          "PERSISTENT_LIQUIDITY",

        side:
          existing.side,

        midpoint:
          existing.midpoint,

        usd:
          existing.currentUsd,

        structuralConfluence:
          existing.structuralConfluence
      });
    }

    if (
      existing.persistent &&
      existing.structuralConfluence &&
      !wasPersistent
    ) {
      addLiquidityEvent({
        type:
          "STRUCTURAL_CONFLUENCE",

        side:
          existing.side,

        midpoint:
          existing.midpoint,

        usd:
          existing.currentUsd,

        structuralLevel:
          existing.structuralLevel
      });
    }
  }

  /*
     Handle clusters that disappeared.
  */
  for (const item of persistenceStore.values()) {
    if (!item.active) continue;

    if (seen.has(item.id)) {
      continue;
    }

    const lifetime =
      ts -
      new Date(
        item.firstSeen
      ).getTime();

    const meaningfulRemoval =
      item.observations >=
        SETTINGS.persistenceRemovedMinObservations ||
      lifetime >=
        SETTINGS.persistenceRemovedMinLifetimeMs;

    if (meaningfulRemoval) {
      item.removed = true;
      item.active = false;
      item.lastSeen = iso();

      if (
        item.persistent ||
        item.observations >= 2
      ) {
        addLiquidityEvent({
          type:
            "LIQUIDITY_REMOVED",

          side:
            item.side,

          midpoint:
            item.midpoint,

          usd:
            item.currentUsd,

          durationSec:
            Math.round(lifetime / 1000)
        });
      }
    }
  }

  /*
     Cleanup old removed entries.
  */
  for (const [id, item] of persistenceStore.entries()) {
    const age =
      ts -
      new Date(
        item.lastSeen
      ).getTime();

    if (
      item.removed &&
      age >
        SETTINGS.persistenceMaxAgeMs
    ) {
      persistenceStore.delete(id);
    }
  }
}

/* ============================================================
   PERSISTENT LIQUIDITY OUTPUT
============================================================ */

function getPersistentLiquidity() {
  const result = {
    bids: [],
    asks: []
  };

  for (const item of persistenceStore.values()) {
    if (
      !item.active ||
      !item.persistent
    ) {
      continue;
    }

    const output = {
      side: item.side,

      priceLow:
        item.priceLow,

      priceHigh:
        item.priceHigh,

      midpoint:
        item.midpoint,

      strongestPrice:
        item.strongestPrice,

      strongestBucketUsd:
        item.strongestBucketUsd,

      usd:
        item.currentUsd,

      maxUsd:
        item.maxUsd,

      previousUsd:
        item.previousUsd,

      changeUsd:
        item.changeUsd || 0,

      firstSeen:
        item.firstSeen,

      lastSeen:
        item.lastSeen,

      durationMs:
        item.durationMs,

      durationSec:
        item.durationSec,

      observations:
        item.observations,

      status:
        item.status,

      removed:
        item.removed,

      persistent:
        item.persistent,

      structuralConfluence:
        item.structuralConfluence,

      structuralLevel:
        item.structuralLevel,

      structuralTimeframes:
        item.structuralTimeframes,

      structuralStrength:
        item.structuralStrength
    };

    if (item.side === "bid") {
      result.bids.push(output);
    } else {
      result.asks.push(output);
    }
  }

  result.bids.sort(
    (a, b) => b.usd - a.usd
  );

  result.asks.sort(
    (a, b) => b.usd - a.usd
  );

  return result;
}

/* ============================================================
   ORDER BOOK OUTPUT
============================================================ */

function getOrderBookIntelligence() {
  const bidsOutput =
    buildBookClusters("bid")
      .map(
        enrichClusterWithConfluence
      );

  const asksOutput =
    buildBookClusters("ask")
      .map(
        enrichClusterWithConfluence
      );

  return {
    bids: bidsOutput,
    asks: asksOutput
  };
}

/* ============================================================
   FORMAT STRUCTURE
============================================================ */

function formatLevel(level, price) {
  return {
    side:
      level.side,

    price:
      level.price,

    priceLow:
      level.priceLow,

    priceHigh:
      level.priceHigh,

    equalCount:
      level.equalCount,

    reactions:
      level.reactions,

    timeframes:
      level.timeframes,

    timeframeWeight:
      level.timeframeWeight,

    scoreBase:
      level.scoreBase,

    score:
      level.score,

    restingUsd:
      level.restingUsd || 0,

    restingConfirmed:
      Boolean(
        level.restingConfirmed
      ),

    restingLevels:
      level.restingLevels || [],

    strength:
      level.strength,

    distancePct:
      pctDistance(
        level.price,
        price
      )
  };
}

/* ============================================================
   INTELLIGENCE
============================================================ */

function getIntelligence() {
  const price =
    currentPrice ||
    getMidPrice();

  const majorBSL =
    getDirectionalMajor("BSL");

  const majorSSL =
    getDirectionalMajor("SSL");

  const nearBSL =
    getDirectionalNear("BSL");

  const nearSSL =
    getDirectionalNear("SSL");

  const nearestBSL =
    majorBSL.length
      ? formatLevel(
          majorBSL[0],
          price
        )
      : null;

  const nearestSSL =
    majorSSL.length
      ? formatLevel(
          majorSSL[0],
          price
        )
      : null;

  let state =
    "NO MAJOR STRUCTURAL LIQUIDITY";

  if (
    nearestBSL &&
    nearestSSL
  ) {
    state =
      "BETWEEN MAJOR LIQUIDITY";
  }

  return {
    state,

    structureStatus: {
      activeLevels:
        structure.levels.length,

      activeMajorBSL:
        majorBSL.length,

      activeMajorSSL:
        majorSSL.length,

      lastGoodAt:
        structure.lastGoodAt,

      lastRebuildAccepted:
        structure.lastRebuildAccepted,

      lastRebuildReason:
        structure.lastRebuildReason,

      rebuildCount:
        structure.rebuildCount,

      rejectedRebuilds:
        structure.rejectedRebuilds,

      lastCandidateLevels:
        structure.lastCandidateLevels,

      lastCandidateMajor:
        structure.lastCandidateMajor,

      retainedPrevious:
        structure.retainedPrevious,

      status:
        structure.status
    },

    nearestBSL,

    nearestSSL,

    nextBSL:
      majorBSL
        .slice(1, 6)
        .map(
          (x) =>
            formatLevel(
              x,
              price
            )
        ),

    nextSSL:
      majorSSL
        .slice(1, 6)
        .map(
          (x) =>
            formatLevel(
              x,
              price
            )
        ),

    nearBSL:
      nearBSL
        .slice(0, 5)
        .map(
          (x) =>
            formatLevel(
              x,
              price
            )
        ),

    nearSSL:
      nearSSL
        .slice(0, 5)
        .map(
          (x) =>
            formatLevel(
              x,
              price
            )
        ),

    orderBook:
      getOrderBookIntelligence(),

    persistentLiquidity:
      getPersistentLiquidity(),

    liquidityEvents:
      liquidityEvents.slice(0, 25),

    recentSweeps:
      recentSweeps.slice(0, 25)
  };
}

/* ============================================================
   STRUCTURE OUTPUT
============================================================ */

function getStructureOutput() {
  const price =
    currentPrice ||
    getMidPrice();

  return {
    status:
      structure.status,

    currentPrice:
      price,

    levels:
      structure.levels.map(
        (x) =>
          formatLevel(x, price)
      ),

    majorLevels:
      structure.majorLevels.map(
        (x) =>
          formatLevel(x, price)
      ),

    majorBSL:
      getDirectionalMajor("BSL")
        .map(
          (x) =>
            formatLevel(x, price)
        ),

    majorSSL:
      getDirectionalMajor("SSL")
        .map(
          (x) =>
            formatLevel(x, price)
        ),

    lastGoodAt:
      structure.lastGoodAt
  };
}

/* ============================================================
   HEALTH
============================================================ */

function getHealth() {
  return {
    ok: true,

    service:
      "BTCUSDT Liquidity Relay",

    version:
      "12.0",

    symbol:
      SYMBOL,

    status:
      initialized
        ? "live"
        : "syncing",

    websocketConnected:
      websocketConnected,

    depthConnected:
      depthConnected,

    snapshotConnected:
      snapshotConnected,

    klineConnected:
      klineConnected,

    initialized:
      initialized,

    waitingForBridge:
      waitingForBridge,

    snapshotPending:
      snapshotPending,

    snapshotInFlight:
      snapshotInFlight,

    currentPrice:
      currentPrice,

    priceSource:
      currentPrice
        ? "futures_orderbook_mid"
        : null,

    bidLevels:
      bids.size,

    askLevels:
      asks.size,

    lastUpdateId:
      lastUpdateId,

    pendingEvents:
      pendingEvents.length,

    snapshotRequests:
      snapshotRequests,

    snapshot429s:
      snapshot429s,

    bridgeAttempts:
      bridgeAttempts,

    bridgeFound:
      bridgeFound,

    resyncs:
      resyncs,

    sequenceGaps:
      sequenceGaps,

    lastSnapshotId:
      lastSnapshotId,

    lastBridgeU:
      lastBridgeU,

    lastAppliedEventU:
      lastAppliedEventU,

    lastAppliedEventUStart:
      lastAppliedEventUStart,

    lastGapExpected:
      lastGapExpected,

    lastGapReceived:
      lastGapReceived,

    syncGeneration:
      syncGeneration,

    resyncScheduled:
      resyncScheduled,

    structure: {
      status:
        structure.status,

      activeLevels:
        structure.levels.length,

      activeMajorBSL:
        getDirectionalMajor("BSL")
          .length,

      activeMajorSSL:
        getDirectionalMajor("SSL")
          .length,

      lastGoodAt:
        structure.lastGoodAt,

      lastRebuildAccepted:
        structure.lastRebuildAccepted,

      lastRebuildReason:
        structure.lastRebuildReason,

      rebuildCount:
        structure.rebuildCount,

      rejectedRebuilds:
        structure.rejectedRebuilds,

      lastCandidateLevels:
        structure.lastCandidateLevels,

      lastCandidateMajor:
        structure.lastCandidateMajor,

      retainedPrevious:
        structure.retainedPrevious
    },

    startedAt,

    updatedAt:
      iso(),

    lastError
  };
}

/* ============================================================
   ENDPOINTS
============================================================ */

app.get("/", (req, res) => {
  res.json({
    ok: true,
    service:
      "BTCUSDT Liquidity Relay",
    version:
      "12.0",
    endpoints: [
      "/health",
      "/book",
      "/liquidity",
      "/structure",
      "/intelligence",
      "/events"
    ]
  });
});

app.get("/health", (req, res) => {
  res.json(
    getHealth()
  );
});

app.get("/book", (req, res) => {
  res.json({
    ok: true,
    symbol: SYMBOL,
    currentPrice:
      currentPrice ||
      getMidPrice(),

    initialized,

    orderBook:
      getOrderBookIntelligence(),

    updatedAt:
      iso()
  });
});

app.get("/liquidity", (req, res) => {
  res.json({
    ok: true,
    symbol: SYMBOL,

    currentPrice:
      currentPrice ||
      getMidPrice(),

    orderBook:
      getOrderBookIntelligence(),

    persistentLiquidity:
      getPersistentLiquidity(),

    updatedAt:
      iso()
  });
});

app.get("/structure", (req, res) => {
  res.json({
    ok: true,
    symbol: SYMBOL,

    ...getStructureOutput(),

    updatedAt:
      iso()
  });
});

app.get("/intelligence", (req, res) => {
  res.json({
    ok: true,

    symbol: SYMBOL,

    currentPrice:
      currentPrice ||
      getMidPrice(),

    priceSource:
      currentPrice
        ? "futures_orderbook_mid"
        : null,

    intelligence:
      getIntelligence(),

    updatedAt:
      iso()
  });
});

app.get("/events", (req, res) => {
  res.json({
    ok: true,

    symbol: SYMBOL,

    recentSweeps:
      recentSweeps.slice(0, 50),

    liquidityEvents:
      liquidityEvents.slice(0, 50),

    persistentLiquidity:
      getPersistentLiquidity(),

    updatedAt:
      iso()
  });
});

/* ============================================================
   ERROR HANDLING
============================================================ */

function handleError(err) {
  if (!err) return;

  lastError =
    err.message ||
    String(err);
}

/* ============================================================
   PERIODIC TASKS
============================================================ */

setInterval(() => {
  try {
    updatePersistence();
  } catch (err) {
    handleError(err);
  }
}, 1000);

setInterval(() => {
  try {
    const price =
      currentPrice ||
      getMidPrice();

    if (price && structure.levels.length === 0) {
      rebuildStructure(
        "periodic_recovery"
      );
    }

    /*
       If structure has been retained too long,
       rebuild it.
    */
    if (
      structure.lastGoodAt &&
      now() -
        new Date(
          structure.lastGoodAt
        ).getTime() >
        SETTINGS.structureCacheMaxAgeMs
    ) {
      rebuildStructure(
        "structure_cache_expired"
      );
    }
  } catch (err) {
    handleError(err);
  }
}, 30000);

/* ============================================================
   STARTUP
============================================================ */

async function startup() {
  console.log(
    "================================================="
  );

  console.log(
    "BTCUSDT LIQUIDITY RELAY V12 STARTING"
  );

  console.log(
    "================================================="
  );

  try {
    await loadHistoricalCandles();

    rebuildStructure(
      "initial_historical_load"
    );
  } catch (err) {
    handleError(err);
  }

  openSnapshotSocket();

  connectDepth();

  connectKlines();
}

app.listen(PORT, () => {
  console.log(
    `Server listening on port ${PORT}`
  );

  startup().catch(handleError);
});
