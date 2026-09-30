const express = require("express");
const WebSocket = require("ws");

const app = express();

const PORT = process.env.PORT || 10000;
const SYMBOL = "BTCUSDT";
const SYMBOL_LOWER = SYMBOL.toLowerCase();

/* ============================================================
   BINANCE CONNECTIONS
   ============================================================ */

const DEPTH_WS =
  `wss://fstream.binance.com/ws/${SYMBOL_LOWER}@depth@100ms`;

const KLINE_WS =
  `wss://fstream.binance.com/stream?streams=` +
  `${SYMBOL_LOWER}@kline_15m/` +
  `${SYMBOL_LOWER}@kline_1h/` +
  `${SYMBOL_LOWER}@kline_4h/` +
  `${SYMBOL_LOWER}@kline_1d`;

const WS_API =
  "wss://ws-fapi.binance.com/ws-fapi/v1";

/* ============================================================
   GENERAL SETTINGS
   ============================================================ */

const RANGE_PCT = 0.05;

/*
   We deliberately do NOT use the old "dense cluster = major
   liquidity" logic as the primary signal anymore.
*/

const RAW_BOOK_MIN_USD = 50000;

const MAX_PENDING_EVENTS = 2000;

const SNAPSHOT_MIN_INTERVAL_MS = 65000;
const SNAPSHOT_TIMEOUT_MS = 15000;

/* ============================================================
   STRUCTURE SETTINGS
   ============================================================ */

const STRUCTURE = {
  "15m": {
    interval: "15m",
    weight: 1,
    candles: 300,
    pivotLeft: 3,
    pivotRight: 3,
    equalTolerancePct: 0.0015
  },

  "1h": {
    interval: "1h",
    weight: 2,
    candles: 300,
    pivotLeft: 4,
    pivotRight: 4,
    equalTolerancePct: 0.0015
  },

  "4h": {
    interval: "4h",
    weight: 3,
    candles: 250,
    pivotLeft: 5,
    pivotRight: 5,
    equalTolerancePct: 0.002
  },

  "1d": {
    interval: "1d",
    weight: 4,
    candles: 180,
    pivotLeft: 5,
    pivotRight: 5,
    equalTolerancePct: 0.0025
  }
};

/*
   A structural level is allowed a price zone around the exact
   swing. This prevents tiny price differences from producing
   dozens of separate levels.
*/
const LEVEL_ZONE_PCT = 0.0015;

/*
   Price must come reasonably close to a level for a reaction/
   touch to count.
*/
const TOUCH_TOLERANCE_PCT = 0.0015;

/*
   Distance from a swing required before counting a meaningful
   reaction.
*/
const REACTION_MOVE_PCT = 0.003;

/*
   How much current resting liquidity must exist around a
   structural level before it is considered meaningful.
*/
const MIN_STRUCTURAL_RESTING_USD = 250000;

/*
   Maximum distance from a structural price to inspect the
   current order book.
*/
const RESTING_LIQUIDITY_RADIUS_USD = 25;

/*
   Merge nearby swing prices into one structural zone.
*/
const MERGE_LEVEL_DISTANCE_USD = 25;

/*
   Maximum number of major levels returned per side.
*/
const MAX_MAJOR_LEVELS = 8;

/* ============================================================
   ORDER BOOK
   ============================================================ */

const bids = new Map();
const asks = new Map();

let currentPrice = null;

/* ============================================================
   DEPTH SYNCHRONIZATION
   ============================================================ */

let depthWs = null;
let snapshotWs = null;

let initialized = false;
let waitingForBridge = false;
let snapshotPending = false;
let gapRecovery = false;
let gapDetected = false;

let pendingDepthEvents = [];

let lastUpdateId = 0;

let snapshotRequests = 0;
let snapshot429s = 0;
let bridgeAttempts = 0;
let bridgeFound = 0;
let gapCount = 0;

let lastGapLocal = null;
let lastGapIncoming = null;

let lastSnapshotRequest = null;
let lastSnapshotResponse = null;
let lastSnapshotError = null;
let lastSnapshotId = null;

let snapshotTimer = null;
let snapshotTimeout = null;

let snapshotRequestId = 0;

/* ============================================================
   CANDLE DATA
   ============================================================ */

const candles = {
  "15m": [],
  "1h": [],
  "4h": [],
  "1d": []
};

let klineWs = null;

/* ============================================================
   STRUCTURAL LEVELS
   ============================================================ */

let swingHighs = [];
let swingLows = [];

let lastStructureUpdate = null;

/* ============================================================
   HELPERS
   ============================================================ */

function nowIso() {
  return new Date().toISOString();
}

function safeNumber(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function round(value, decimals = 2) {
  const p = Math.pow(10, decimals);
  return Math.round(value * p) / p;
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function pctDistance(price, reference) {
  if (!reference) return 0;
  return ((price - reference) / reference) * 100;
}

function absolutePctDistance(price, reference) {
  return Math.abs(pctDistance(price, reference));
}

function usdValue(price, quantity) {
  return price * quantity;
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/* ============================================================
   ORDER BOOK HELPERS
   ============================================================ */

function applyBookSide(map, updates) {
  if (!Array.isArray(updates)) return;

  for (const item of updates) {
    if (!Array.isArray(item) || item.length < 2) continue;

    const price = safeNumber(item[0]);
    const quantity = safeNumber(item[1]);

    if (price === null || quantity === null) continue;

    if (quantity === 0) {
      map.delete(price);
    } else {
      map.set(price, quantity);
    }
  }
}

function applyDepthEvent(event) {
  if (!event) return;

  applyBookSide(bids, event.b);
  applyBookSide(asks, event.a);

  if (event.u !== undefined) {
    lastUpdateId = Number(event.u);
  }
}

function getBestBid() {
  let best = null;

  for (const [price, quantity] of bids) {
    if (quantity <= 0) continue;

    if (best === null || price > best.price) {
      best = {
        price,
        quantity
      };
    }
  }

  return best;
}

function getBestAsk() {
  let best = null;

  for (const [price, quantity] of asks) {
    if (quantity <= 0) continue;

    if (best === null || price < best.price) {
      best = {
        price,
        quantity
      };
    }
  }

  return best;
}

function getBookMid() {
  const bid = getBestBid();
  const ask = getBestAsk();

  if (bid && ask) {
    return (bid.price + ask.price) / 2;
  }

  if (bid) return bid.price;
  if (ask) return ask.price;

  return currentPrice;
}

/* ============================================================
   DEPTH EVENT BUFFER
   ============================================================ */

function bufferDepthEvent(event) {
  pendingDepthEvents.push(event);

  if (pendingDepthEvents.length > MAX_PENDING_EVENTS) {
    pendingDepthEvents.splice(
      0,
      pendingDepthEvents.length - MAX_PENDING_EVENTS
    );
  }
}

/* ============================================================
   SNAPSHOT BRIDGE
   ============================================================ */

function findBridgeEvent(snapshotId) {
  const target = Number(snapshotId) + 1;

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

function tryCompleteBridge() {
  if (!waitingForBridge) return false;
  if (!lastSnapshotId) return false;

  bridgeAttempts++;

  const bridge = findBridgeEvent(lastSnapshotId);

  if (!bridge) {
    return false;
  }

  bridgeFound++;

  applyDepthEvent(bridge);

  initialized = true;
  waitingForBridge = false;
  snapshotPending = false;
  gapRecovery = false;
  gapDetected = false;

  const bridgeIndex = pendingDepthEvents.indexOf(bridge);

  if (bridgeIndex >= 0) {
    pendingDepthEvents =
      pendingDepthEvents.slice(bridgeIndex + 1);
  } else {
    pendingDepthEvents = [];
  }

  return true;
}

/* ============================================================
   SNAPSHOT REQUEST
   ============================================================ */

function canRequestSnapshot() {
  if (snapshotPending) return false;

  if (!lastSnapshotRequest) return true;

  const elapsed =
    Date.now() - new Date(lastSnapshotRequest).getTime();

  return elapsed >= SNAPSHOT_MIN_INTERVAL_MS;
}

function requestSnapshot(reason = "startup") {
  if (!canRequestSnapshot()) {
    return;
  }

  if (!snapshotWs || snapshotWs.readyState !== WebSocket.OPEN) {
    return;
  }

  snapshotPending = true;
  waitingForBridge = false;

  snapshotRequests++;

  lastSnapshotRequest = nowIso();

  const id = String(++snapshotRequestId);

  const request = {
    id,
    method: "depth",
    params: {
      symbol: SYMBOL,
      limit: 1000
    }
  };

  try {
    snapshotWs.send(JSON.stringify(request));
  } catch (error) {
    snapshotPending = false;
    lastSnapshotError = error.message;
    return;
  }

  if (snapshotTimeout) {
    clearTimeout(snapshotTimeout);
  }

  snapshotTimeout = setTimeout(() => {
    if (!snapshotPending) return;

    snapshotPending = false;
    lastSnapshotError = "Snapshot request timeout";

    scheduleSnapshotRecovery();
  }, SNAPSHOT_TIMEOUT_MS);
}

function installSnapshot(result) {
  if (!result) return false;

  const snapshotId = Number(result.lastUpdateId);

  if (!Number.isFinite(snapshotId)) {
    lastSnapshotError = "Invalid snapshot lastUpdateId";
    return false;
  }

  const snapshotBids = Array.isArray(result.bids)
    ? result.bids
    : [];

  const snapshotAsks = Array.isArray(result.asks)
    ? result.asks
    : [];

  bids.clear();
  asks.clear();

  for (const item of snapshotBids) {
    if (!Array.isArray(item)) continue;

    const price = safeNumber(item[0]);
    const quantity = safeNumber(item[1]);

    if (
      price !== null &&
      quantity !== null &&
      quantity > 0
    ) {
      bids.set(price, quantity);
    }
  }

  for (const item of snapshotAsks) {
    if (!Array.isArray(item)) continue;

    const price = safeNumber(item[0]);
    const quantity = safeNumber(item[1]);

    if (
      price !== null &&
      quantity !== null &&
      quantity > 0
    ) {
      asks.set(price, quantity);
    }
  }

  lastSnapshotId = snapshotId;
  lastUpdateId = snapshotId;

  initialized = false;
  waitingForBridge = true;
  gapRecovery = false;

  lastSnapshotResponse = nowIso();
  lastSnapshotError = null;

  if (snapshotTimeout) {
    clearTimeout(snapshotTimeout);
    snapshotTimeout = null;
  }

  tryCompleteBridge();

  return true;
}

/* ============================================================
   DEPTH EVENT PROCESSING
   ============================================================ */

function handleDepthEvent(event) {
  if (!event || event.e !== "depthUpdate") {
    return;
  }

  if (!currentPrice) {
    const bid = safeNumber(
      event.b && event.b[0] ? event.b[0][0] : null
    );

    const ask = safeNumber(
      event.a && event.a[0] ? event.a[0][0] : null
    );

    if (bid !== null && ask !== null) {
      currentPrice = (bid + ask) / 2;
    }
  }

  /*
     Always keep a bounded recent buffer.
     This is important for snapshot bridging.
  */
  bufferDepthEvent(event);

  /*
     If we don't have a valid book yet, wait for snapshot bridge.
  */
  if (!initialized) {
    tryCompleteBridge();
    return;
  }

  const U = Number(event.U);
  const u = Number(event.u);

  if (!Number.isFinite(U) || !Number.isFinite(u)) {
    return;
  }

  /*
     Normal expected sequence:
     U <= lastUpdateId + 1
     u >= lastUpdateId + 1
  */
  if (
    U <= lastUpdateId + 1 &&
    u >= lastUpdateId + 1
  ) {
    applyDepthEvent(event);

    if (gapRecovery) {
      gapRecovery = false;
      gapDetected = false;
    }

    /*
       Once applied, old events are no longer needed.
    */
    while (pendingDepthEvents.length > 0) {
      const first = pendingDepthEvents[0];

      if (Number(first.u) <= lastUpdateId) {
        pendingDepthEvents.shift();
      } else {
        break;
      }
    }

    return;
  }

  /*
     Event is completely behind our current update.
  */
  if (u <= lastUpdateId) {
    return;
  }

  /*
     Genuine forward gap.
  */
  gapCount++;

  gapDetected = true;
  gapRecovery = true;

  lastGapLocal = lastUpdateId;
  lastGapIncoming = U;

  /*
     IMPORTANT:
     Do NOT destroy the current book.
     We retain it until a valid snapshot bridge arrives.
  */

  scheduleSnapshotRecovery();
}

/* ============================================================
   SNAPSHOT RECOVERY
   ============================================================ */

function scheduleSnapshotRecovery() {
  if (snapshotTimer) {
    return;
  }

  const delay = canRequestSnapshot()
    ? 1000
    : Math.max(
        1000,
        SNAPSHOT_MIN_INTERVAL_MS -
        (
          Date.now() -
          new Date(lastSnapshotRequest).getTime()
        )
      );

  snapshotTimer = setTimeout(() => {
    snapshotTimer = null;

    requestSnapshot("gap-recovery");
  }, delay);
}

/* ============================================================
   DEPTH WEBSOCKET
   ============================================================ */

function connectDepthWebSocket() {
  if (depthWs) {
    try {
      depthWs.close();
    } catch (e) {}
  }

  depthWs = new WebSocket(DEPTH_WS);

  depthWs.on("open", () => {
    /*
       Snapshot WebSocket handles synchronization.
       Market depth stream only supplies updates.
    */

    if (!initialized) {
      requestSnapshot("depth-connected");
    }
  });

  depthWs.on("message", raw => {
    try {
      const event = JSON.parse(raw.toString());

      handleDepthEvent(event);
    } catch (error) {
      console.error(
        "Depth parse error:",
        error.message
      );
    }
  });

  depthWs.on("close", () => {
    setTimeout(() => {
      connectDepthWebSocket();
    }, 3000);
  });

  depthWs.on("error", error => {
    console.error(
      "Depth WebSocket error:",
      error.message
    );
  });
}

/* ============================================================
   SNAPSHOT WEBSOCKET
   ============================================================ */

function connectSnapshotWebSocket() {
  if (snapshotWs) {
    try {
      snapshotWs.close();
    } catch (e) {}
  }

  snapshotWs = new WebSocket(WS_API);

  snapshotWs.on("open", () => {
    /*
       Depth stream may already be receiving events.
       Request snapshot after the stream is active.
    */

    requestSnapshot("snapshot-connected");
  });

  snapshotWs.on("message", raw => {
    try {
      const message = JSON.parse(raw.toString());

      if (
        message.status === 429 ||
        (
          message.error &&
          (
            message.error.code === -1003 ||
            message.error.code === 429
          )
        )
      ) {
        snapshot429s++;
        snapshotPending = false;

        lastSnapshotError =
          message.error?.msg ||
          "Snapshot rate limit";

        scheduleSnapshotRecovery();

        return;
      }

      if (
        message.status === 200 &&
        message.result
      ) {
        installSnapshot(message.result);
        return;
      }

      if (message.error) {
        snapshotPending = false;

        lastSnapshotError =
          message.error.msg ||
          "Snapshot API error";

        scheduleSnapshotRecovery();
      }

    } catch (error) {
      lastSnapshotError = error.message;
    }
  });

  snapshotWs.on("close", () => {
    setTimeout(() => {
      connectSnapshotWebSocket();
    }, 3000);
  });

  snapshotWs.on("error", error => {
    console.error(
      "Snapshot WebSocket error:",
      error.message
    );
  });
}

/* ============================================================
   KLINE WEBSOCKET
   ============================================================ */

function connectKlineWebSocket() {
  if (klineWs) {
    try {
      klineWs.close();
    } catch (e) {}
  }

  klineWs = new WebSocket(KLINE_WS);

  klineWs.on("open", () => {
    console.log("Kline WebSocket connected");
  });

  klineWs.on("message", raw => {
    try {
      const wrapper = JSON.parse(raw.toString());

      const data = wrapper.data;

      if (
        !data ||
        data.e !== "kline"
      ) {
        return;
      }

      const k = data.k;

      if (!k) return;

      const interval = k.i;

      if (!candles[interval]) {
        return;
      }

      const candle = {
        openTime: Number(k.t),
        closeTime: Number(k.T),
        open: Number(k.o),
        high: Number(k.h),
        low: Number(k.l),
        close: Number(k.c),
        volume: Number(k.v),
        closed: Boolean(k.x)
      };

      upsertCandle(interval, candle);

    } catch (error) {
      console.error(
        "Kline parse error:",
        error.message
      );
    }
  });

  klineWs.on("close", () => {
    setTimeout(() => {
      connectKlineWebSocket();
    }, 3000);
  });

  klineWs.on("error", error => {
    console.error(
      "Kline WebSocket error:",
      error.message
    );
  });
}

/* ============================================================
   KLINE DATA
   ============================================================ */

function upsertCandle(interval, candle) {
  if (!candles[interval]) {
    candles[interval] = [];
  }

  const list = candles[interval];

  const index = list.findIndex(
    x => x.openTime === candle.openTime
  );

  if (index >= 0) {
    list[index] = candle;
  } else {
    list.push(candle);
  }

  list.sort(
    (a, b) => a.openTime - b.openTime
  );

  const max =
    STRUCTURE[interval]?.candles || 300;

  if (list.length > max) {
    list.splice(0, list.length - max);
  }

  if (candle.closed) {
    rebuildStructure();
  }
}

/* ============================================================
   HISTORICAL KLINES THROUGH WEBSOCKET API
   ============================================================ */

let klineRequestId = 100000;

function requestHistoricalKlines(interval) {
  if (
    !snapshotWs ||
    snapshotWs.readyState !== WebSocket.OPEN
  ) {
    return;
  }

  const config = STRUCTURE[interval];

  if (!config) return;

  const id = String(++klineRequestId);

  const request = {
    id,
    method: "klines",
    params: {
      symbol: SYMBOL,
      interval: config.interval,
      limit: config.candles
    }
  };

  try {
    snapshotWs.send(
      JSON.stringify(request)
    );

    historicalKlineRequests.set(id, interval);

  } catch (error) {
    console.error(
      "Historical kline request error:",
      error.message
    );
  }
}

const historicalKlineRequests = new Map();

/*
   Patch snapshot message handling for historical klines.
*/
function handleHistoricalKlineResponse(message) {
  if (!message || !message.id) {
    return false;
  }

  const id = String(message.id);

  if (!historicalKlineRequests.has(id)) {
    return false;
  }

  const interval =
    historicalKlineRequests.get(id);

  historicalKlineRequests.delete(id);

  if (
    message.status !== 200 ||
    !Array.isArray(message.result)
  ) {
    return true;
  }

  candles[interval] =
    message.result
      .map(row => ({
        openTime: Number(row[0]),
        open: Number(row[1]),
        high: Number(row[2]),
        low: Number(row[3]),
        close: Number(row[4]),
        volume: Number(row[5]),
        closeTime: Number(row[6]),
        closed: true
      }))
      .filter(x =>
        Number.isFinite(x.openTime) &&
        Number.isFinite(x.high) &&
        Number.isFinite(x.low)
      );

  rebuildStructure();

  return true;
}

/* ============================================================
   STRUCTURE CALCULATIONS
   ============================================================ */

function trueRange(candle, previous) {
  if (!previous) {
    return candle.high - candle.low;
  }

  return Math.max(
    candle.high - candle.low,
    Math.abs(candle.high - previous.close),
    Math.abs(candle.low - previous.close)
  );
}

function averageTrueRange(list, period = 14) {
  if (!list || list.length < 2) {
    return null;
  }

  const ranges = [];

  for (let i = 1; i < list.length; i++) {
    ranges.push(
      trueRange(
        list[i],
        list[i - 1]
      )
    );
  }

  const start =
    Math.max(0, ranges.length - period);

  const selected =
    ranges.slice(start);

  if (!selected.length) {
    return null;
  }

  return (
    selected.reduce(
      (sum, value) => sum + value,
      0
    ) / selected.length
  );
}

/* ============================================================
   SWING DETECTION
   ============================================================ */

function detectSwings(interval, list) {
  const config = STRUCTURE[interval];

  if (!config || !list) {
    return {
      highs: [],
      lows: []
    };
  }

  const highs = [];
  const lows = [];

  const left = config.pivotLeft;
  const right = config.pivotRight;

  if (
    list.length <
    left + right + 5
  ) {
    return {
      highs,
      lows
    };
  }

  const atr =
    averageTrueRange(list, 14);

  for (
    let i = left;
    i < list.length - right;
    i++
  ) {
    const c = list[i];

    let isHigh = true;
    let isLow = true;

    for (
      let j = i - left;
      j <= i + right;
      j++
    ) {
      if (j === i) continue;

      if (list[j].high >= c.high) {
        isHigh = false;
      }

      if (list[j].low <= c.low) {
        isLow = false;
      }
    }

    if (isHigh) {
      const future =
        list.slice(
          i + 1,
          Math.min(
            list.length,
            i + right + 30
          )
        );

      let reaction = 0;

      for (const x of future) {
        if (
          x.low <=
          c.high *
          (1 - REACTION_MOVE_PCT)
        ) {
          reaction++;
        }
      }

      highs.push({
        type: "swing_high",
        side: "buy_side",
        interval,
        price: c.high,
        time: c.openTime,
        index: i,
        timeframeWeight: config.weight,
        reactionCount: reaction,
        atr
      });
    }

    if (isLow) {
      const future =
        list.slice(
          i + 1,
          Math.min(
            list.length,
            i + right + 30
          )
        );

      let reaction = 0;

      for (const x of future) {
        if (
          x.high >=
          c.low *
          (1 + REACTION_MOVE_PCT)
        ) {
          reaction++;
        }
      }

      lows.push({
        type: "swing_low",
        side: "sell_side",
        interval,
        price: c.low,
        time: c.openTime,
        index: i,
        timeframeWeight: config.weight,
        reactionCount: reaction,
        atr
      });
    }
  }

  return {
    highs,
    lows
  };
}

/* ============================================================
   EQUAL HIGH / LOW DETECTION
   ============================================================ */

function addEqualLiquidity(swingList, tolerancePct) {
  const result = [];

  for (let i = 0; i < swingList.length; i++) {
    const base = swingList[i];

    let matches = 1;
    let priceSum = base.price;

    for (let j = i + 1; j < swingList.length; j++) {
      const other = swingList[j];

      if (
        Math.abs(
          other.price - base.price
        ) /
        base.price <=
        tolerancePct
      ) {
        matches++;
        priceSum += other.price;
      }
    }

    result.push({
      ...base,
      equalCount: matches,
      averagePrice:
        priceSum / matches
    });
  }

  return result;
}

/* ============================================================
   STRUCTURAL LEVEL MERGING
   ============================================================ */

function mergeLevels(levels) {
  if (!levels.length) {
    return [];
  }

  const sorted =
    [...levels].sort(
      (a, b) => a.price - b.price
    );

  const groups = [];

  for (const level of sorted) {
    let group = null;

    for (const g of groups) {
      if (
        Math.abs(
          level.price - g.price
        ) <= MERGE_LEVEL_DISTANCE_USD
      ) {
        group = g;
        break;
      }
    }

    if (!group) {
      group = {
        price: level.price,
        levels: []
      };

      groups.push(group);
    }

    group.levels.push(level);

    group.price =
      group.levels.reduce(
        (sum, x) => sum + x.price,
        0
      ) /
      group.levels.length;
  }

  return groups;
}

/* ============================================================
   STRUCTURAL LEVEL SCORING
   ============================================================ */

function scoreStructuralGroup(group, side) {
  const levels = group.levels;

  let timeframeScore = 0;
  let reactions = 0;
  let equalCount = 0;

  const intervals = new Set();

  for (const level of levels) {
    timeframeScore +=
      level.timeframeWeight || 1;

    reactions +=
      level.reactionCount || 0;

    equalCount =
      Math.max(
        equalCount,
        level.equalCount || 1
      );

    intervals.add(level.interval);
  }

  /*
     Multi-timeframe agreement is important.
  */
  const timeframeCount =
    intervals.size;

  let score = 0;

  score +=
    timeframeScore * 10;

  score +=
    reactions * 2;

  score +=
    Math.max(0, equalCount - 1) * 12;

  score +=
    Math.max(0, timeframeCount - 1) * 15;

  if (side === "buy") {
    score += 5;
  } else {
    score += 5;
  }

  let importance = "MODERATE";

  if (score >= 100) {
    importance = "VERY HIGH";
  } else if (score >= 70) {
    importance = "HIGH";
  } else if (score >= 45) {
    importance = "MEDIUM";
  }

  return {
    score,
    importance,
    timeframeCount,
    intervals: [...intervals],
    reactions,
    equalCount
  };
}

/* ============================================================
   REBUILD ALL STRUCTURE
   ============================================================ */

function rebuildStructure() {
  const allHighs = [];
  const allLows = [];

  for (const interval of Object.keys(STRUCTURE)) {
    const list = candles[interval];

    const result =
      detectSwings(
        interval,
        list
      );

    const highs =
      addEqualLiquidity(
        result.highs,
        STRUCTURE[interval]
          .equalTolerancePct
      );

    const lows =
      addEqualLiquidity(
        result.lows,
        STRUCTURE[interval]
          .equalTolerancePct
      );

    allHighs.push(...highs);
    allLows.push(...lows);
  }

  swingHighs = allHighs;
  swingLows = allLows;

  lastStructureUpdate = nowIso();
}

/* ============================================================
   CURRENT RESTING LIQUIDITY AROUND A LEVEL
   ============================================================ */

function restingLiquidityAround(
  price,
  side
) {
  const map =
    side === "sell"
      ? asks
      : bids;

  const levels = [];

  let totalUsd = 0;
  let quantity = 0;
  let strongestUsd = 0;
  let strongestPrice = null;

  for (const [levelPrice, levelQty] of map) {
    const distance =
      Math.abs(
        levelPrice - price
      );

    if (
      distance >
      RESTING_LIQUIDITY_RADIUS_USD
    ) {
      continue;
    }

    const usd =
      usdValue(
        levelPrice,
        levelQty
      );

    if (
      usd <
      RAW_BOOK_MIN_USD
    ) {
      continue;
    }

    totalUsd += usd;
    quantity += levelQty;

    if (usd > strongestUsd) {
      strongestUsd = usd;
      strongestPrice = levelPrice;
    }

    levels.push({
      price: levelPrice,
      quantity: levelQty,
      usd
    });
  }

  levels.sort(
    (a, b) =>
      b.usd - a.usd
  );

  return {
    totalUsd,
    quantity,
    strongestUsd,
    strongestPrice,
    levels
  };
}

/* ============================================================
   BUILD STRUCTURAL LEVELS
   ============================================================ */

function buildMajorLevels() {
  if (!currentPrice) {
    return {
      resistance: [],
      support: [],
      bsl: [],
      ssl: []
    };
  }

  const highs =
    swingHighs.filter(
      x =>
        x.price >
        currentPrice
    );

  const lows =
    swingLows.filter(
      x =>
        x.price <
        currentPrice
    );

  const highGroups =
    mergeLevels(highs);

  const lowGroups =
    mergeLevels(lows);

  const bsl = [];

  for (const group of highGroups) {
    const score =
      scoreStructuralGroup(
        group,
        "buy"
      );

    const resting =
      restingLiquidityAround(
        group.price,
        "sell"
      );

    const distancePct =
      absolutePctDistance(
        group.price,
        currentPrice
      );

    /*
       More distant levels are still allowed,
       but levels immediately around price are
       not automatically considered major.
    */

    const structuralStrength =
      score.score;

    const restingBonus =
      resting.totalUsd >=
      MIN_STRUCTURAL_RESTING_USD
        ? 20
        : 0;

    const finalScore =
      structuralStrength +
      restingBonus;

    let classification =
      "STRUCTURAL";

    if (
      score.equalCount >= 2
    ) {
      classification =
        "EQUAL HIGH / BSL";
    }

    if (
      resting.totalUsd >=
      MIN_STRUCTURAL_RESTING_USD
    ) {
      classification +=
        " + RESTING LIQUIDITY";
    }

    bsl.push({
      side: "buy_side",
      type: "BSL",
      price: round(group.price, 2),
      priceLow: round(
        group.price *
        (1 - LEVEL_ZONE_PCT),
        2
      ),
      priceHigh: round(
        group.price *
        (1 + LEVEL_ZONE_PCT),
        2
      ),
      distancePct:
        round(
          pctDistance(
            group.price,
            currentPrice
          ),
          3
        ),
      importance:
        score.importance,
      classification,
      score: finalScore,
      timeframeCount:
        score.timeframeCount,
      intervals:
        score.intervals,
      equalCount:
        score.equalCount,
      reactionCount:
        score.reactions,
      restingSellUsd:
        round(
          resting.totalUsd,
          2
        ),
      strongestRestingSellUsd:
        round(
          resting.strongestUsd,
          2
        ),
      strongestRestingSellPrice:
        resting.strongestPrice,
      restingLevels:
        resting.levels.length
    });
  }

  const ssl = [];

  for (const group of lowGroups) {
    const score =
      scoreStructuralGroup(
        group,
        "sell"
      );

    const resting =
      restingLiquidityAround(
        group.price,
        "buy"
      );

    const distancePct =
      absolutePctDistance(
        group.price,
        currentPrice
      );

    const structuralStrength =
      score.score;

    const restingBonus =
      resting.totalUsd >=
      MIN_STRUCTURAL_RESTING_USD
        ? 20
        : 0;

    const finalScore =
      structuralStrength +
      restingBonus;

    let classification =
      "STRUCTURAL";

    if (
      score.equalCount >= 2
    ) {
      classification =
        "EQUAL LOW / SSL";
    }

    if (
      resting.totalUsd >=
      MIN_STRUCTURAL_RESTING_USD
    ) {
      classification +=
        " + RESTING LIQUIDITY";
    }

    ssl.push({
      side: "sell_side",
      type: "SSL",
      price: round(group.price, 2),
      priceLow: round(
        group.price *
        (1 - LEVEL_ZONE_PCT),
        2
      ),
      priceHigh: round(
        group.price *
        (1 + LEVEL_ZONE_PCT),
        2
      ),
      distancePct:
        round(
          pctDistance(
            group.price,
            currentPrice
          ),
          3
        ),
      importance:
        score.importance,
      classification,
      score: finalScore,
      timeframeCount:
        score.timeframeCount,
      intervals:
        score.intervals,
      equalCount:
        score.equalCount,
      reactionCount:
        score.reactions,
      restingBuyUsd:
        round(
          resting.totalUsd,
          2
        ),
      strongestRestingBuyUsd:
        round(
          resting.strongestUsd,
          2
        ),
      strongestRestingBuyPrice:
        resting.strongestPrice,
      restingLevels:
        resting.levels.length
    });
  }

  /*
     Only keep levels that have enough structural
     evidence. This is the main noise filter.
  */

  const importantBSL =
    bsl
      .filter(
        x =>
          x.score >= 45
      )
      .sort(
        (a, b) =>
          b.score - a.score
      )
      .slice(
        0,
        MAX_MAJOR_LEVELS
      );

  const importantSSL =
    ssl
      .filter(
        x =>
          x.score >= 45
      )
      .sort(
        (a, b) =>
          b.score - a.score
      )
      .slice(
        0,
        MAX_MAJOR_LEVELS
      );

  /*
     Resistance = BSL levels above price.
     Support = SSL levels below price.
  */

  return {
    resistance:
      [...importantBSL].sort(
        (a, b) =>
          a.price - b.price
      ),

    support:
      [...importantSSL].sort(
        (a, b) =>
          b.price - a.price
      ),

    bsl: importantBSL,

    ssl: importantSSL
  };
}

/* ============================================================
   RAW BOOK LIQUIDITY
   ============================================================ */

function buildRawBookLiquidity() {
  if (!currentPrice) {
    return {
      buy: [],
      sell: []
    };
  }

  const lower =
    currentPrice *
    (1 - RANGE_PCT);

  const upper =
    currentPrice *
    (1 + RANGE_PCT);

  const buy = [];

  for (const [price, quantity] of bids) {
    if (
      price < lower ||
      price > currentPrice
    ) {
      continue;
    }

    const usd =
      usdValue(
        price,
        quantity
      );

    if (
      usd >=
      RAW_BOOK_MIN_USD
    ) {
      buy.push({
        price: round(price, 2),
        quantity: round(quantity, 6),
        usd: round(usd, 2)
      });
    }
  }

  const sell = [];

  for (const [price, quantity] of asks) {
    if (
      price > upper ||
      price < currentPrice
    ) {
      continue;
    }

    const usd =
      usdValue(
        price,
        quantity
      );

    if (
      usd >=
      RAW_BOOK_MIN_USD
    ) {
      sell.push({
        price: round(price, 2),
        quantity: round(quantity, 6),
        usd: round(usd, 2)
      });
    }
  }

  buy.sort(
    (a, b) =>
      b.usd - a.usd
  );

  sell.sort(
    (a, b) =>
      b.usd - a.usd
  );

  return {
    buy: buy.slice(0, 50),
    sell: sell.slice(0, 50)
  };
}

/* ============================================================
   BUILD LIQUIDITY MAP
   ============================================================ */

function buildLiquidityMap() {
  const bookPrice =
    getBookMid();

  if (bookPrice) {
    currentPrice = bookPrice;
  }

  const structure =
    buildMajorLevels();

  const raw =
    buildRawBookLiquidity();

  const nearestBSL =
    [...structure.bsl]
      .filter(
        x =>
          x.price >
          currentPrice
      )
      .sort(
        (a, b) =>
          a.price - b.price
      )
      .slice(0, 3);

  const nearestSSL =
    [...structure.ssl]
      .filter(
        x =>
          x.price <
          currentPrice
      )
      .sort(
        (a, b) =>
          b.price - a.price
      )
      .slice(0, 3);

  return {
    symbol: SYMBOL,

    currentPrice:
      currentPrice
        ? round(
            currentPrice,
            2
          )
        : null,

    marketStructure: {
      majorResistance:
        structure.resistance,

      majorSupport:
        structure.support,

      buySideLiquidity:
        structure.bsl,

      sellSideLiquidity:
        structure.ssl,

      nearestBSL,

      nearestSSL
    },

    rawRestingLiquidity: {
      largeBids:
        raw.buy,

      largeAsks:
        raw.sell
    },

    settings: {
      timeframes:
        Object.keys(STRUCTURE),

      levelZonePct:
        LEVEL_ZONE_PCT,

      touchTolerancePct:
        TOUCH_TOLERANCE_PCT,

      reactionMovePct:
        REACTION_MOVE_PCT,

      minStructuralRestingUsd:
        MIN_STRUCTURAL_RESTING_USD,

      restingLiquidityRadiusUsd:
        RESTING_LIQUIDITY_RADIUS_USD,

      mergeLevelDistanceUsd:
        MERGE_LEVEL_DISTANCE_USD
    },

    structureStats: {
      candles15m:
        candles["15m"].length,

      candles1h:
        candles["1h"].length,

      candles4h:
        candles["4h"].length,

      candles1d:
        candles["1d"].length,

      swingHighs:
        swingHighs.length,

      swingLows:
        swingLows.length,

      updatedAt:
        lastStructureUpdate
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
          : "syncing",

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
          WebSocket.OPEN,

      klineWebSocket:
        klineWs &&
        klineWs.readyState ===
          WebSocket.OPEN
    },

    updatedAt:
      nowIso()
  };
}

/* ============================================================
   HEALTH
   ============================================================ */

app.get(
  "/health",
  (req, res) => {
    res.json({
      ok: true,

      symbol: SYMBOL,

      status:
        initialized
          ? (
              gapRecovery
                ? "gap_recovery"
                : "live"
            )
          : "syncing",

      initialized,

      waitingForBridge,

      snapshotPending,

      depthConnected:
        depthWs &&
        depthWs.readyState ===
          WebSocket.OPEN,

      snapshotConnected:
        snapshotWs &&
        snapshotWs.readyState ===
          WebSocket.OPEN,

      klineConnected:
        klineWs &&
        klineWs.readyState ===
          WebSocket.OPEN,

      currentPrice,

      bidLevels:
        bids.size,

      askLevels:
        asks.size,

      pendingEvents:
        pendingDepthEvents.length,

      lastUpdateId,

      resyncs:
        snapshotRequests,

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

      lastSnapshotId,

      structure: {
        candles15m:
          candles["15m"].length,

        candles1h:
          candles["1h"].length,

        candles4h:
          candles["4h"].length,

        candles1d:
          candles["1d"].length,

        swingHighs:
          swingHighs.length,

        swingLows:
          swingLows.length,

        updatedAt:
          lastStructureUpdate
      },

      updatedAt:
        nowIso()
    });
  }
);

/* ============================================================
   LIQUIDITY ENDPOINT
   ============================================================ */

app.get(
  "/liquidity",
  (req, res) => {
    res.json(
      buildLiquidityMap()
    );
  }
);

/* ============================================================
   BOOK ENDPOINT
   ============================================================ */

app.get(
  "/book",
  (req, res) => {
    const bidLevels =
      [...bids.entries()]
        .sort(
          (a, b) =>
            b[0] - a[0]
        )
        .slice(0, 100);

    const askLevels =
      [...asks.entries()]
        .sort(
          (a, b) =>
            a[0] - b[0]
        )
        .slice(0, 100);

    res.json({
      symbol: SYMBOL,

      currentPrice:
        currentPrice
          ? round(
              currentPrice,
              2
            )
          : null,

      bids:
        bidLevels.map(
          ([price, quantity]) => ({
            price,
            quantity,
            usd:
              round(
                usdValue(
                  price,
                  quantity
                ),
                2
              )
          })
        ),

      asks:
        askLevels.map(
          ([price, quantity]) => ({
            price,
            quantity,
            usd:
              round(
                usdValue(
                  price,
                  quantity
                ),
                2
              )
          })
        ),

      initialized,

      lastUpdateId,

      updatedAt:
        nowIso()
    });
  }
);

/* ============================================================
   ROOT
   ============================================================ */

app.get(
  "/",
  (req, res) => {
    res.json({
      ok: true,
      service:
        "Binance Futures Major Liquidity Relay",
      symbol: SYMBOL,

      endpoints: [
        "/health",
        "/liquidity",
        "/book"
      ],

      description:
        "Major swing structure + BSL/SSL + current resting liquidity"
    });
  }
);

/* ============================================================
   PATCH SNAPSHOT MESSAGE HANDLER
   ============================================================ */

/*
   We need to intercept historical kline responses
   without disturbing depth snapshot responses.

   Wrap the original message handler behavior by replacing
   the snapshot message listener below.
*/

function attachSnapshotMessageHandler() {
  if (!snapshotWs) return;

  snapshotWs.removeAllListeners("message");

  snapshotWs.on("message", raw => {
    try {
      const message =
        JSON.parse(
          raw.toString()
        );

      if (
        handleHistoricalKlineResponse(
          message
        )
      ) {
        return;
      }

      if (
        message.status === 429 ||
        (
          message.error &&
          (
            message.error.code === -1003 ||
            message.error.code === 429
          )
        )
      ) {
        snapshot429s++;
        snapshotPending = false;

        lastSnapshotError =
          message.error?.msg ||
          "Snapshot rate limit";

        scheduleSnapshotRecovery();

        return;
      }

      if (
        message.status === 200 &&
        message.result &&
        message.result.lastUpdateId !==
          undefined
      ) {
        installSnapshot(
          message.result
        );

        return;
      }

      if (message.error) {
        snapshotPending = false;

        lastSnapshotError =
          message.error.msg ||
          "Snapshot API error";

        scheduleSnapshotRecovery();
      }

    } catch (error) {
      lastSnapshotError =
        error.message;
    }
  });
}

/* ============================================================
   OVERRIDE SNAPSHOT CONNECTION
   ============================================================ */

function connectSnapshotWebSocketFinal() {
  if (snapshotWs) {
    try {
      snapshotWs.close();
    } catch (e) {}
  }

  snapshotWs =
    new WebSocket(WS_API);

  snapshotWs.on("open", () => {

    attachSnapshotMessageHandler();

    /*
       Request order-book snapshot.
    */
    requestSnapshot(
      "snapshot-connected"
    );

    /*
       Request structural candle history.
       Four requests only at startup/reconnect.
    */

    setTimeout(() => {
      requestHistoricalKlines("15m");
      requestHistoricalKlines("1h");
      requestHistoricalKlines("4h");
      requestHistoricalKlines("1d");
    }, 1000);
  });

  snapshotWs.on("close", () => {

    setTimeout(() => {
      connectSnapshotWebSocketFinal();
    }, 3000);
  });

  snapshotWs.on("error", error => {
    console.error(
      "Snapshot WebSocket error:",
      error.message
    );
  });
}

/* ============================================================
   START
   ============================================================ */

app.listen(
  PORT,
  () => {
    console.log(
      `Binance Futures Major Liquidity Relay running on port ${PORT}`
    );

    console.log(
      `Symbol: ${SYMBOL}`
    );

    console.log(
      "Structure: 15m / 1h / 4h / 1D"
    );

    console.log(
      "Mode: Major Swing + BSL/SSL + Resting Liquidity"
    );
  }
);

/*
   Start market streams.
*/

connectDepthWebSocket();

connectKlineWebSocket();

/*
   Snapshot/API WebSocket is deliberately started separately.
*/

setTimeout(() => {
  connectSnapshotWebSocketFinal();
}, 500);
