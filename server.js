/*
============================================================
BINANCE BTCUSDT MAJOR LIQUIDITY RELAY
============================================================

ARCHITECTURE
------------------------------------------------------------
1. Binance SPOT REST
   -> Historical BTCUSDT candles
   -> Used ONLY to initialize market structure

2. Binance FUTURES WEBSOCKET
   -> Live order book
   -> Live Futures candles
   -> Current Futures price
   -> Actual resting liquidity

3. STRUCTURE ENGINE
   -> Swing highs/lows
   -> Equal highs/lows
   -> BSL / SSL
   -> Major support/resistance
   -> Multi-timeframe scoring

4. REST API
   /health
   /liquidity
   /structure
   /book
============================================================
*/

const express = require("express");
const WebSocket = require("ws");
const axios = require("axios");

const app = express();

const PORT = process.env.PORT || 10000;
const SYMBOL = "BTCUSDT";
const symbolLower = SYMBOL.toLowerCase();

/* =========================================================
   CONFIG
========================================================= */

const SPOT_REST =
  "https://data-api.binance.vision/api/v3/klines";

const FUTURES_WS =
  "wss://fstream.binance.com/ws";

const FUTURES_DEPTH_STREAM =
  `${FUTURES_WS}/${symbolLower}@depth@100ms`;

const FUTURES_KLINE_STREAMS = [
  `${symbolLower}@kline_15m`,
  `${symbolLower}@kline_1h`,
  `${symbolLower}@kline_4h`,
  `${symbolLower}@kline_1d`
];

const FUTURES_COMBINED_KLINE_WS =
  `${FUTURES_WS}/stream?streams=${FUTURES_KLINE_STREAMS.join("/")}`;

/*
Historical candles.

Spot is intentionally used only for initialization.

The live Futures kline stream will replace/update
the latest candles afterward.
*/

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
Order book / structural confirmation.
*/

const RAW_BOOK_MIN_USD = 50000;

const RESTING_LIQUIDITY_RADIUS_USD = 35;

const MIN_STRUCTURAL_RESTING_USD = 250000;

const MERGE_LEVEL_DISTANCE_USD = 35;

const LEVEL_ZONE_PCT = 0.0015;

const REACTION_MOVE_PCT = 0.003;

const MIN_MAJOR_LEVEL_SCORE = 40;

const MAX_MAJOR_LEVELS = 8;

/*
Snapshot protection.
*/

const SNAPSHOT_MIN_INTERVAL_MS = 65000;

const MAX_PENDING_EVENTS = 20000;


/* =========================================================
   STATE
========================================================= */

let currentPrice = null;

let futuresDepthWs = null;
let futuresKlineWs = null;

let depthReconnectTimer = null;
let klineReconnectTimer = null;

let snapshotWs = null;
let snapshotPending = false;

let lastSnapshotRequest = 0;
let snapshotRequests = 0;
let snapshot429s = 0;
let snapshotId = 0;

let snapshotLastError = null;
let snapshotLastStatus = null;

let lastSnapshotResponse = null;

let initialized = false;

let gapRecovery = false;
let gapDetected = false;
let gapCount = 0;
let lastGapLocal = null;
let lastGapIncoming = null;


/* =========================================================
   ORDER BOOK
========================================================= */

const bids = new Map();
const asks = new Map();

let depthLastUpdateId = 0;

let pendingDepthEvents = [];

let depthConnected = false;
let snapshotConnected = false;


/* =========================================================
   CANDLE STORAGE
========================================================= */

const candleHistory = {
  "15m": [],
  "1h": [],
  "4h": [],
  "1d": []
};

const candleSource = {
  "15m": "none",
  "1h": "none",
  "4h": "none",
  "1d": "none"
};

const candleRequests = {
  "15m": 0,
  "1h": 0,
  "4h": 0,
  "1d": 0
};

const candleSuccess = {
  "15m": 0,
  "1h": 0,
  "4h": 0,
  "1d": 0
};

const candleErrors = {
  "15m": 0,
  "1h": 0,
  "4h": 0,
  "1d": 0
};

let candleLastStatus = null;
let candleLastError = null;


/* =========================================================
   STRUCTURE
========================================================= */

const structure = {
  swingHighs: [],
  swingLows: [],
  equalHighs: [],
  equalLows: [],
  levels: [],
  majorBSL: [],
  majorSSL: [],
  majorResistance: [],
  majorSupport: [],
  updatedAt: null
};


/* =========================================================
   UTILITIES
========================================================= */

function now() {
  return new Date().toISOString();
}

function num(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function pctDistance(a, b) {
  if (!a || !b) return 999;

  return Math.abs(a - b) / b;
}

function priceDistancePct(a, b) {
  if (!a || !b) return 999;

  return ((a - b) / b) * 100;
}

function safeArray(value) {
  return Array.isArray(value) ? value : [];
}


/* =========================================================
   CANDLE NORMALIZATION
========================================================= */

function normalizeSpotKline(k) {
  if (!Array.isArray(k) || k.length < 6) {
    return null;
  }

  return {
    openTime: Number(k[0]),
    open: Number(k[1]),
    high: Number(k[2]),
    low: Number(k[3]),
    close: Number(k[4]),
    volume: Number(k[5]),
    closeTime: Number(k[6] || 0),
    source: "spot"
  };
}

function normalizeFuturesKline(k) {
  if (!k) return null;

  return {
    openTime: Number(k.t),
    open: Number(k.o),
    high: Number(k.h),
    low: Number(k.l),
    close: Number(k.c),
    volume: Number(k.v),
    closeTime: Number(k.T || 0),
    source: "futures"
  };
}


/* =========================================================
   HISTORICAL SPOT CANDLES
========================================================= */

async function loadHistoricalCandles(tf) {
  const cfg = STRUCTURE[tf];

  if (!cfg) return;

  candleRequests[tf]++;

  try {
    const response = await axios.get(SPOT_REST, {
      params: {
        symbol: SYMBOL,
        interval: cfg.interval,
        limit: cfg.candles
      },

      timeout: 15000,

      headers: {
        "User-Agent":
          "Mozilla/5.0 Binance-Major-Liquidity-Relay"
      }
    });

    const rows = safeArray(response.data);

    const candles = rows
      .map(normalizeSpotKline)
      .filter(Boolean)
      .sort((a, b) => a.openTime - b.openTime);

    if (!candles.length) {
      throw new Error("No historical candles returned");
    }

    candleHistory[tf] = candles;

    candleSource[tf] = "binance_spot";

    candleSuccess[tf]++;

    candleLastStatus = response.status;
    candleLastError = null;

    console.log(
      `[CANDLE] ${tf} loaded ${candles.length} Spot candles`
    );

    return true;

  } catch (err) {

    candleErrors[tf]++;

    candleLastStatus =
      err.response?.status ||
      err.code ||
      null;

    candleLastError =
      err.response?.data
        ? JSON.stringify(err.response.data)
        : String(err.message || err);

    console.log(
      `[CANDLE ERROR] ${tf}`,
      candleLastStatus,
      candleLastError
    );

    return false;
  }
}


/* =========================================================
   LOAD ALL HISTORICAL STRUCTURE
========================================================= */

async function loadAllHistoricalCandles() {

  console.log(
    "[STRUCTURE] Loading historical Spot candles..."
  );

  for (const tf of Object.keys(STRUCTURE)) {

    await loadHistoricalCandles(tf);

    /*
    Small pause between requests.
    */

    await new Promise(resolve =>
      setTimeout(resolve, 300)
    );
  }

  rebuildStructure();

  console.log(
    "[STRUCTURE] Historical structure initialized"
  );
}


/* =========================================================
   FUTURES LIVE KLINES
========================================================= */

function connectFuturesKlines() {

  if (futuresKlineWs) {

    try {
      futuresKlineWs.close();
    } catch (_) {}
  }

  console.log(
    "[KLINE] Connecting Binance Futures kline stream..."
  );

  futuresKlineWs =
    new WebSocket(FUTURES_COMBINED_KLINE_WS);

  futuresKlineWs.on("open", () => {

    console.log(
      "[KLINE] Binance Futures connected"
    );
  });

  futuresKlineWs.on("message", raw => {

    try {

      const message =
        JSON.parse(raw.toString());

      /*
      Combined stream:
      {
        stream: "...",
        data: {...}
      }
      */

      const data =
        message.data || message;

      if (data.e !== "kline") {
        return;
      }

      const k = data.k;

      if (!k) return;

      const interval = k.i;

      let tf = null;

      if (interval === "15m") tf = "15m";
      if (interval === "1h") tf = "1h";
      if (interval === "4h") tf = "4h";
      if (interval === "1d") tf = "1d";

      if (!tf) return;

      const candle =
        normalizeFuturesKline(k);

      if (!candle) return;

      /*
      Replace the current candle if it exists.
      Otherwise append it.
      */

      const history =
        candleHistory[tf];

      const existingIndex =
        history.findIndex(
          x => x.openTime === candle.openTime
        );

      if (existingIndex >= 0) {

        history[existingIndex] =
          candle;

      } else {

        history.push(candle);

        history.sort(
          (a, b) =>
            a.openTime - b.openTime
        );
      }

      /*
      Keep memory bounded.
      */

      const max =
        STRUCTURE[tf].candles + 50;

      if (history.length > max) {

        candleHistory[tf] =
          history.slice(
            history.length - max
          );
      }

      candleSource[tf] =
        "binance_futures_live";

      /*
      Current Futures price.
      */

      currentPrice =
        candle.close;

      /*
      Recalculate structure on candle close.
      */

      if (k.x === true) {

        rebuildStructure();
      }

    } catch (err) {

      console.log(
        "[KLINE MESSAGE ERROR]",
        err.message
      );
    }
  });

  futuresKlineWs.on("error", err => {

    console.log(
      "[KLINE ERROR]",
      err.message
    );
  });

  futuresKlineWs.on("close", () => {

    console.log(
      "[KLINE] Disconnected"
    );

    if (klineReconnectTimer) {
      clearTimeout(klineReconnectTimer);
    }

    klineReconnectTimer =
      setTimeout(
        connectFuturesKlines,
        5000
      );
  });
}


/* =========================================================
   PIVOT DETECTION
========================================================= */

function isPivotHigh(
  candles,
  index,
  left,
  right
) {

  if (
    index - left < 0 ||
    index + right >= candles.length
  ) {
    return false;
  }

  const value =
    candles[index].high;

  for (
    let i = index - left;
    i <= index + right;
    i++
  ) {

    if (i === index) continue;

    if (
      candles[i].high >= value
    ) {
      return false;
    }
  }

  return true;
}


function isPivotLow(
  candles,
  index,
  left,
  right
) {

  if (
    index - left < 0 ||
    index + right >= candles.length
  ) {
    return false;
  }

  const value =
    candles[index].low;

  for (
    let i = index - left;
    i <= index + right;
    i++
  ) {

    if (i === index) continue;

    if (
      candles[i].low <= value
    ) {
      return false;
    }
  }

  return true;
}


/* =========================================================
   STRUCTURE EXTRACTION
========================================================= */

function extractStructure(tf) {

  const cfg =
    STRUCTURE[tf];

  const candles =
    candleHistory[tf] || [];

  if (
    candles.length <
    cfg.pivotLeft +
    cfg.pivotRight +
    10
  ) {

    return {
      highs: [],
      lows: []
    };
  }

  const highs = [];
  const lows = [];

  for (
    let i = cfg.pivotLeft;
    i <
    candles.length - cfg.pivotRight;
    i++
  ) {

    if (
      isPivotHigh(
        candles,
        i,
        cfg.pivotLeft,
        cfg.pivotRight
      )
    ) {

      highs.push({
        timeframe: tf,
        price: candles[i].high,
        time: candles[i].openTime,
        type: "swing_high"
      });
    }

    if (
      isPivotLow(
        candles,
        i,
        cfg.pivotLeft,
        cfg.pivotRight
      )
    ) {

      lows.push({
        timeframe: tf,
        price: candles[i].low,
        time: candles[i].openTime,
        type: "swing_low"
      });
    }
  }

  return {
    highs,
    lows
  };
}


/* =========================================================
   EQUAL LEVEL DETECTION
========================================================= */

function findEqualLevels(levels, tolerancePct) {

  const groups = [];

  for (const level of levels) {

    let group = null;

    for (const existing of groups) {

      const distance =
        pctDistance(
          level.price,
          existing.price
        );

      if (
        distance <= tolerancePct
      ) {

        group = existing;
        break;
      }
    }

    if (!group) {

      group = {
        price: level.price,
        members: []
      };

      groups.push(group);
    }

    group.members.push(level);

    /*
    Keep group price centered.
    */

    group.price =
      group.members.reduce(
        (sum, x) =>
          sum + x.price,
        0
      ) /
      group.members.length;
  }

  return groups
    .filter(
      x => x.members.length >= 2
    );
}


/* =========================================================
   REACTION COUNT
========================================================= */

function calculateReactionCount(
  levelPrice,
  candles
) {

  let reactions = 0;

  if (!candles.length) {
    return 0;
  }

  const zone =
    levelPrice *
    LEVEL_ZONE_PCT;

  for (let i = 0; i < candles.length - 3; i++) {

    const candle =
      candles[i];

    const touched =
      candle.high >=
        levelPrice - zone &&
      candle.low <=
        levelPrice + zone;

    if (!touched) continue;

    const futureEnd =
      Math.min(
        i + 4,
        candles.length - 1
      );

    let maxHigh =
      candle.high;

    let minLow =
      candle.low;

    for (
      let j = i + 1;
      j <= futureEnd;
      j++
    ) {

      maxHigh =
        Math.max(
          maxHigh,
          candles[j].high
        );

      minLow =
        Math.min(
          minLow,
          candles[j].low
        );
    }

    const upwardMove =
      (maxHigh - levelPrice) /
      levelPrice;

    const downwardMove =
      (levelPrice - minLow) /
      levelPrice;

    if (
      upwardMove >=
        REACTION_MOVE_PCT ||
      downwardMove >=
        REACTION_MOVE_PCT
    ) {

      reactions++;
    }
  }

  return reactions;
}


/* =========================================================
   BUILD STRUCTURAL LEVELS
========================================================= */

function rebuildStructure() {

  const allHighs = [];
  const allLows = [];

  const equalHighs = [];
  const equalLows = [];

  for (const tf of Object.keys(STRUCTURE)) {

    const extracted =
      extractStructure(tf);

    allHighs.push(
      ...extracted.highs
    );

    allLows.push(
      ...extracted.lows
    );

    const equalH =
      findEqualLevels(
        extracted.highs,
        STRUCTURE[tf]
          .equalTolerancePct
      );

    const equalL =
      findEqualLevels(
        extracted.lows,
        STRUCTURE[tf]
          .equalTolerancePct
      );

    for (const group of equalH) {

      equalHighs.push({
        timeframe: tf,
        price: group.price,
        count: group.members.length,
        members: group.members
      });
    }

    for (const group of equalL) {

      equalLows.push({
        timeframe: tf,
        price: group.price,
        count: group.members.length,
        members: group.members
      });
    }
  }

  structure.swingHighs =
    allHighs;

  structure.swingLows =
    allLows;

  structure.equalHighs =
    equalHighs;

  structure.equalLows =
    equalLows;

  /*
  Merge nearby highs.
  */

  const highGroups =
    mergeStructuralLevels(
      allHighs,
      "BSL"
    );

  /*
  Merge nearby lows.
  */

  const lowGroups =
    mergeStructuralLevels(
      allLows,
      "SSL"
    );

  /*
  Score.
  */

  const scoredHighs =
    highGroups.map(
      level =>
        scoreStructuralLevel(
          level,
          "BSL"
        )
    );

  const scoredLows =
    lowGroups.map(
      level =>
        scoreStructuralLevel(
          level,
          "SSL"
        )
    );

  structure.majorBSL =
    scoredHighs
      .filter(
        x =>
          x.score >=
          MIN_MAJOR_LEVEL_SCORE
      )
      .sort(
        (a, b) =>
          b.score - a.score
      )
      .slice(
        0,
        MAX_MAJOR_LEVELS
      );

  structure.majorSSL =
    scoredLows
      .filter(
        x =>
          x.score >=
          MIN_MAJOR_LEVEL_SCORE
      )
      .sort(
        (a, b) =>
          b.score - a.score
      )
      .slice(
        0,
        MAX_MAJOR_LEVELS
      );

  structure.majorResistance =
    structure.majorBSL
      .slice()
      .sort(
        (a, b) =>
          Math.abs(
            a.price -
            (currentPrice || a.price)
          ) -
          Math.abs(
            b.price -
            (currentPrice || b.price)
          )
      )
      .slice(
        0,
        3
      );

  structure.majorSupport =
    structure.majorSSL
      .slice()
      .sort(
        (a, b) =>
          Math.abs(
            a.price -
            (currentPrice || a.price)
          ) -
          Math.abs(
            b.price -
            (currentPrice || b.price)
          )
      )
      .slice(
        0,
        3
      );

  structure.updatedAt =
    now();
}


/* =========================================================
   MERGE STRUCTURAL LEVELS
========================================================= */

function mergeStructuralLevels(
  levels,
  side
) {

  if (!levels.length) {
    return [];
  }

  const sorted =
    levels
      .slice()
      .sort(
        (a, b) =>
          a.price - b.price
      );

  const groups = [];

  for (const level of sorted) {

    let group = null;

    for (const existing of groups) {

      if (
        Math.abs(
          level.price -
          existing.price
        ) <=
        MERGE_LEVEL_DISTANCE_USD
      ) {

        group = existing;
        break;
      }
    }

    if (!group) {

      group = {
        side,
        priceLow: level.price,
        priceHigh: level.price,
        members: [],
        timeframes: new Set()
      };

      groups.push(group);
    }

    group.members.push(level);

    group.priceLow =
      Math.min(
        group.priceLow,
        level.price
      );

    group.priceHigh =
      Math.max(
        group.priceHigh,
        level.price
      );

    group.timeframes.add(
      level.timeframe
    );
  }

  return groups.map(group => {

    const avgPrice =
      group.members.reduce(
        (sum, x) =>
          sum + x.price,
        0
      ) /
      group.members.length;

    return {
      side: group.side,
      price: avgPrice,
      priceLow: group.priceLow,
      priceHigh: group.priceHigh,
      members: group.members,
      timeframes:
        Array.from(
          group.timeframes
        )
    };
  });
}


/* =========================================================
   STRUCTURAL SCORE
========================================================= */

function scoreStructuralLevel(
  level,
  side
) {

  let score = 0;

  /*
  Timeframe importance.
  */

  const timeframeScore =
    level.timeframes.reduce(
      (sum, tf) =>
        sum +
        (STRUCTURE[tf]?.weight || 0) *
        10,
      0
    );

  score +=
    timeframeScore;

  /*
  Multi-timeframe agreement.
  */

  if (
    level.timeframes.length >= 2
  ) {

    score +=
      level.timeframes.length *
      8;
  }

  if (
    level.timeframes.length >= 3
  ) {

    score += 10;
  }

  /*
  Number of structural touches.
  */

  score +=
    Math.min(
      level.members.length * 3,
      15
    );

  /*
  Reaction history.
  */

  let reactions = 0;

  for (
    const tf of level.timeframes
  ) {

    reactions +=
      calculateReactionCount(
        level.price,
        candleHistory[tf] || []
      );
  }

  score +=
    Math.min(
      reactions * 2,
      15
    );

  /*
  Equal highs/lows.
  */

  let equalCount = 0;

  if (side === "BSL") {

    for (
      const eq of structure.equalHighs
    ) {

      if (
        pctDistance(
          eq.price,
          level.price
        ) <= LEVEL_ZONE_PCT
      ) {

        equalCount +=
          eq.count;
      }
    }

  } else {

    for (
      const eq of structure.equalLows
    ) {

      if (
        pctDistance(
          eq.price,
          level.price
        ) <= LEVEL_ZONE_PCT
      ) {

        equalCount +=
          eq.count;
      }
    }
  }

  score +=
    Math.min(
      equalCount * 6,
      18
    );

  /*
  Futures resting liquidity confirmation.
  */

  const resting =
    getRestingLiquidityNearLevel(
      level.price,
      side
    );

  const restingUsd =
    resting.totalUsd;

  if (
    restingUsd >=
    MIN_STRUCTURAL_RESTING_USD
  ) {

    score += 15;

  } else if (
    restingUsd >= 100000
  ) {

    score += 7;
  }

  return {
    ...level,

    score: Math.round(score),

    reactions,

    equalCount,

    restingUsd,

    restingLevels:
      resting.levels,

    source:
      "spot_structure+futures_liquidity"
  };
}


/* =========================================================
   RESTING LIQUIDITY NEAR STRUCTURAL LEVEL
========================================================= */

function getRestingLiquidityNearLevel(
  price,
  side
) {

  const source =
    side === "BSL"
      ? asks
      : bids;

  let totalUsd = 0;

  const levels = [];

  for (
    const [
      priceString,
      quantity
    ] of source.entries()
  ) {

    const p =
      Number(priceString);

    const q =
      Number(quantity);

    if (
      !Number.isFinite(p) ||
      !Number.isFinite(q)
    ) {
      continue;
    }

    if (
      Math.abs(
        p - price
      ) >
      RESTING_LIQUIDITY_RADIUS_USD
    ) {
      continue;
    }

    const usd =
      p * q;

    if (
      usd <
      RAW_BOOK_MIN_USD
    ) {
      continue;
    }

    totalUsd += usd;

    levels.push({
      price: p,
      quantity: q,
      usd
    });
  }

  levels.sort(
    (a, b) =>
      b.usd - a.usd
  );

  return {
    totalUsd,
    levels:
      levels.slice(
        0,
        10
      )
  };
}


/* =========================================================
   FUTURES ORDER BOOK
========================================================= */

function applyDepthUpdate(data) {

  if (!data) return;

  const U = Number(data.U);
  const u = Number(data.u);

  if (
    !Number.isFinite(U) ||
    !Number.isFinite(u)
  ) {
    return;
  }

  /*
  First update.
  */

  if (
    !initialized
  ) {

    pendingDepthEvents.push(data);

    if (
      pendingDepthEvents.length >
      MAX_PENDING_EVENTS
    ) {

      pendingDepthEvents =
        pendingDepthEvents.slice(
          -MAX_PENDING_EVENTS
        );
    }

    return;
  }

  /*
  Gap detection.
  */

  const expected =
    depthLastUpdateId + 1;

  if (
    U > expected
  ) {

    lastGapLocal =
      depthLastUpdateId;

    lastGapIncoming =
      U;

    gapDetected = true;

    if (!gapRecovery) {

      gapRecovery = true;

      gapCount++;

      console.log(
        "[DEPTH] GAP DETECTED",
        {
          expected,
          U,
          u
        }
      );
    }

    /*
    Do not destroy the existing book.
    Wait for snapshot recovery.
    */

    return;
  }

  /*
  Ignore stale update.
  */

  if (
    u <= depthLastUpdateId
  ) {
    return;
  }

  /*
  Apply bids.
  */

  for (
    const item of safeArray(data.b)
  ) {

    const price =
      Number(item[0]);

    const qty =
      Number(item[1]);

    if (
      !Number.isFinite(price) ||
      !Number.isFinite(qty)
    ) {
      continue;
    }

    if (qty === 0) {

      bids.delete(
        item[0]
      );

    } else {

      bids.set(
        item[0],
        qty
      );
    }
  }

  /*
  Apply asks.
  */

  for (
    const item of safeArray(data.a)
  ) {

    const price =
      Number(item[0]);

    const qty =
      Number(item[1]);

    if (
      !Number.isFinite(price) ||
      !Number.isFinite(qty)
    ) {
      continue;
    }

    if (qty === 0) {

      asks.delete(
        item[0]
      );

    } else {

      asks.set(
        item[0],
        qty
      );
    }
  }

  depthLastUpdateId =
    u;

  updateCurrentPrice();
}


/* =========================================================
   CURRENT PRICE
========================================================= */

function updateCurrentPrice() {

  let bestBid = null;
  let bestAsk = null;

  for (
    const [
      priceString
    ] of bids.entries()
  ) {

    const p =
      Number(priceString);

    if (
      bestBid === null ||
      p > bestBid
    ) {

      bestBid = p;
    }
  }

  for (
    const [
      priceString
    ] of asks.entries()
  ) {

    const p =
      Number(priceString);

    if (
      bestAsk === null ||
      p < bestAsk
    ) {

      bestAsk = p;
    }
  }

  if (
    bestBid !== null &&
    bestAsk !== null
  ) {

    currentPrice =
      (bestBid + bestAsk) / 2;
  }
}


/* =========================================================
   INSTALL SNAPSHOT
========================================================= */

function installSnapshot(
  snapshot
) {

  if (!snapshot) {
    return false;
  }

  const lastUpdateId =
    Number(
      snapshot.lastUpdateId
    );

  if (
    !Number.isFinite(lastUpdateId)
  ) {

    return false;
  }

  bids.clear();
  asks.clear();

  for (
    const item of safeArray(
      snapshot.bids
    )
  ) {

    const price =
      Number(item[0]);

    const qty =
      Number(item[1]);

    if (
      Number.isFinite(price) &&
      Number.isFinite(qty) &&
      qty > 0
    ) {

      bids.set(
        item[0],
        qty
      );
    }
  }

  for (
    const item of safeArray(
      snapshot.asks
    )
  ) {

    const price =
      Number(item[0]);

    const qty =
      Number(item[1]);

    if (
      Number.isFinite(price) &&
      Number.isFinite(qty) &&
      qty > 0
    ) {

      asks.set(
        item[0],
        qty
      );
    }
  }

  depthLastUpdateId =
    lastUpdateId;

  initialized = true;

  snapshotId =
    lastUpdateId;

  /*
  Try to bridge buffered events.
  */

  const buffered =
    pendingDepthEvents
      .slice()
      .sort(
        (a, b) =>
          Number(a.U) -
          Number(b.U)
      );

  pendingDepthEvents = [];

  /*
  Find first event that bridges
  snapshot + 1.
  */

  const target =
    lastUpdateId + 1;

  let bridgeIndex = -1;

  for (
    let i = 0;
    i < buffered.length;
    i++
  ) {

    const U =
      Number(
        buffered[i].U
      );

    const u =
      Number(
        buffered[i].u
      );

    if (
      U <= target &&
      u >= target
    ) {

      bridgeIndex = i;

      break;
    }
  }

  if (
    bridgeIndex >= 0
  ) {

    for (
      let i = bridgeIndex;
      i < buffered.length;
      i++
    ) {

      applyDepthUpdate(
        buffered[i]
      );
    }

    gapRecovery = false;
    gapDetected = false;

  } else {

    /*
    No bridge.

    Keep snapshot alive but mark recovery.
    */

    gapRecovery = true;
    gapDetected = true;
  }

  updateCurrentPrice();

  return true;
}


/* =========================================================
   SNAPSHOT WEBSOCKET
========================================================= */

function requestSnapshot() {

  const nowMs =
    Date.now();

  if (
    snapshotPending
  ) {

    return;
  }

  if (
    nowMs -
      lastSnapshotRequest <
    SNAPSHOT_MIN_INTERVAL_MS
  ) {

    return;
  }

  lastSnapshotRequest =
    nowMs;

  snapshotPending = true;

  snapshotRequests++;

  console.log(
    "[SNAPSHOT] Requesting Futures depth snapshot..."
  );

  try {

    if (snapshotWs) {

      try {
        snapshotWs.close();
      } catch (_) {}
    }

    snapshotWs =
      new WebSocket(
        "wss://ws-fapi.binance.com/ws-fapi/v1"
      );

    snapshotWs.on(
      "open",
      () => {

        snapshotConnected =
          true;

        const requestId =
          String(
            Date.now()
          );

        snapshotWs.send(
          JSON.stringify({
            id: requestId,
            method: "depth",
            params: {
              symbol: SYMBOL,
              limit: 1000
            }
          })
        );
      }
    );

    snapshotWs.on(
      "message",
      raw => {

        try {

          const response =
            JSON.parse(
              raw.toString()
            );

          if (
            response.status &&
            response.status !== 200
          ) {

            snapshotLastStatus =
              response.status;

            snapshotLastError =
              JSON.stringify(
                response.error ||
                response
              );

            if (
              response.status === 429
            ) {

              snapshot429s++;
            }

            snapshotPending =
              false;

            return;
          }

          if (
            response.result &&
            response.result.lastUpdateId
          ) {

            lastSnapshotResponse =
              now();

            snapshotLastStatus =
              200;

            snapshotLastError =
              null;

            installSnapshot(
              response.result
            );

            snapshotPending =
              false;

            console.log(
              "[SNAPSHOT] Installed",
              response.result.lastUpdateId,
              "bids:",
              response.result.bids?.length,
              "asks:",
              response.result.asks?.length
            );
          }

        } catch (err) {

          snapshotLastError =
            err.message;

          snapshotPending =
            false;
        }
      }
    );

    snapshotWs.on(
      "error",
      err => {

        snapshotLastError =
          err.message;

        snapshotPending =
          false;

        console.log(
          "[SNAPSHOT ERROR]",
          err.message
        );
      }
    );

    snapshotWs.on(
      "close",
      () => {

        snapshotConnected =
          false;
      }
    );

  } catch (err) {

    snapshotLastError =
      err.message;

    snapshotPending =
      false;
  }
}


/* =========================================================
   DEPTH CONNECTION
========================================================= */

function connectDepth() {

  if (futuresDepthWs) {

    try {
      futuresDepthWs.close();
    } catch (_) {}
  }

  console.log(
    "[DEPTH] Connecting Binance Futures..."
  );

  futuresDepthWs =
    new WebSocket(
      FUTURES_DEPTH_STREAM
    );

  futuresDepthWs.on(
    "open",
    () => {

      depthConnected =
        true;

      console.log(
        "[DEPTH] Connected"
      );

      /*
      Initial snapshot.
      */

      requestSnapshot();
    }
  );

  futuresDepthWs.on(
    "message",
    raw => {

      try {

        const data =
          JSON.parse(
            raw.toString()
          );

        /*
        Futures depth event.
        */

        if (
          data.e === "depthUpdate"
        ) {

          applyDepthUpdate(
            data
          );
        }

      } catch (err) {

        console.log(
          "[DEPTH MESSAGE ERROR]",
          err.message
        );
      }
    }
  );

  futuresDepthWs.on(
    "error",
    err => {

      console.log(
        "[DEPTH ERROR]",
        err.message
      );
    }
  );

  futuresDepthWs.on(
    "close",
    () => {

      depthConnected =
        false;

      console.log(
        "[DEPTH] Disconnected"
      );

      if (
        depthReconnectTimer
      ) {

        clearTimeout(
          depthReconnectTimer
        );
      }

      depthReconnectTimer =
        setTimeout(
          connectDepth,
          5000
        );
    }
  );
}


/* =========================================================
   RESYNC LOOP
========================================================= */

setInterval(
  () => {

    /*
    If there was a genuine gap,
    request a new snapshot,
    respecting the 65-second cooldown.
    */

    if (
      gapRecovery
    ) {

      requestSnapshot();
    }

  },
  5000
);


/* =========================================================
   PERIODIC STRUCTURE REFRESH
========================================================= */

setInterval(
  () => {

    rebuildStructure();

  },
  30000
);


/* =========================================================
   RAW ORDER BOOK SUMMARY
========================================================= */

function getRawRestingLiquidity() {

  const largeBids = [];
  const largeAsks = [];

  for (
    const [
      priceString,
      quantity
    ] of bids.entries()
  ) {

    const price =
      Number(priceString);

    const qty =
      Number(quantity);

    const usd =
      price * qty;

    if (
      usd >=
      RAW_BOOK_MIN_USD
    ) {

      largeBids.push({
        price,
        quantity: qty,
        usd
      });
    }
  }

  for (
    const [
      priceString,
      quantity
    ] of asks.entries()
  ) {

    const price =
      Number(priceString);

    const qty =
      Number(quantity);

    const usd =
      price * qty;

    if (
      usd >=
      RAW_BOOK_MIN_USD
    ) {

      largeAsks.push({
        price,
        quantity: qty,
        usd
      });
    }
  }

  largeBids.sort(
    (a, b) =>
      b.usd - a.usd
  );

  largeAsks.sort(
    (a, b) =>
      b.usd - a.usd
  );

  return {

    largeBids:
      largeBids.slice(
        0,
        25
      ),

    largeAsks:
      largeAsks.slice(
        0,
        25
      )
  };
}


/* =========================================================
   MAJOR LIQUIDITY MAP
========================================================= */

function getLiquidityMap() {

  const bsl =
    structure.majorBSL
      .slice()
      .sort(
        (a, b) =>
          a.price - b.price
      );

  const ssl =
    structure.majorSSL
      .slice()
      .sort(
        (a, b) =>
          b.price - a.price
      );

  /*
  Nearest major resistance above price.
  */

  const resistance =
    bsl
      .filter(
        x =>
          !currentPrice ||
          x.price >
          currentPrice
      )
      .sort(
        (a, b) =>
          a.price - b.price
      )
      .slice(
        0,
        3
      );

  /*
  Nearest major support below price.
  */

  const support =
    ssl
      .filter(
        x =>
          !currentPrice ||
          x.price <
          currentPrice
      )
      .sort(
        (a, b) =>
          b.price - a.price
      )
      .slice(
        0,
        3
      );

  return {

    buySideLiquidity:
      bsl,

    sellSideLiquidity:
      ssl,

    majorResistance:
      resistance,

    majorSupport:
      support,

    nearestBSL:
      resistance[0]
        ? [resistance[0]]
        : [],

    nearestSSL:
      support[0]
        ? [support[0]]
        : []
  };
}


/* =========================================================
   FORMAT LEVEL
========================================================= */

function formatLevel(level) {

  return {

    side:
      level.side,

    price:
      Number(
        level.price.toFixed(2)
      ),

    priceLow:
      Number(
        level.priceLow.toFixed(2)
      ),

    priceHigh:
      Number(
        level.priceHigh.toFixed(2)
      ),

    score:
      level.score,

    timeframes:
      level.timeframes,

    equalCount:
      level.equalCount,

    reactions:
      level.reactions,

    restingUsd:
      Number(
        level.restingUsd.toFixed(2)
      ),

    restingLevels:
      level.restingLevels,

    distancePct:
      currentPrice
        ? Number(
            (
              (
                level.price -
                currentPrice
              ) /
              currentPrice
            ) *
            100
          ).toFixed(3)
        )
        : null,

    source:
      level.source
  };
}


/* =========================================================
   HEALTH
========================================================= */

app.get(
  "/health",
  (req, res) => {

    res.json({

      ok: true,

      service:
        "Binance BTCUSDT Major Liquidity Relay",

      symbol:
        SYMBOL,

      historicalStructureSource:
        "Binance Spot",

      liveStructureSource:
        "Binance Futures WebSocket",

      liquiditySource:
        "Binance Futures Order Book",

      currentPrice,

      candleHistory: {
        "15m":
          candleHistory["15m"].length,

        "1h":
          candleHistory["1h"].length,

        "4h":
          candleHistory["4h"].length,

        "1d":
          candleHistory["1d"].length
      },

      candleSource,

      candleRequests,

      candleSuccess,

      candleErrors,

      candleLastStatus,

      candleLastError,

      structure: {
        swingHighs:
          structure.swingHighs.length,

        swingLows:
          structure.swingLows.length,

        equalHighs:
          structure.equalHighs.length,

        equalLows:
          structure.equalLows.length,

        majorBSL:
          structure.majorBSL.length,

        majorSSL:
          structure.majorSSL.length,

        updatedAt:
          structure.updatedAt
      },

      orderBook: {

        initialized,

        bids:
          bids.size,

        asks:
          asks.size,

        lastUpdateId:
          depthLastUpdateId
      },

      connections: {

        depth:
          depthConnected,

        snapshot:
          snapshotConnected,

        klines:
          !!(
            futuresKlineWs &&
            futuresKlineWs.readyState ===
            WebSocket.OPEN
          )
      },

      sync: {

        gapRecovery,

        gapDetected,

        gapCount,

        lastGapLocal,

        lastGapIncoming,

        pendingEvents:
          pendingDepthEvents.length,

        snapshotPending,

        snapshotId,

        snapshotRequests,

        snapshot429s,

        lastSnapshotRequest:
          lastSnapshotRequest
            ? new Date(
                lastSnapshotRequest
              ).toISOString()
            : null,

        lastSnapshotResponse,

        snapshotLastStatus,

        snapshotLastError
      },

      updatedAt:
        now()
    });
  }
);


/* =========================================================
   STRUCTURE ENDPOINT
========================================================= */

app.get(
  "/structure",
  (req, res) => {

    res.json({

      ok: true,

      symbol:
        SYMBOL,

      currentPrice,

      historicalSource:
        "Binance Spot",

      liveSource:
        "Binance Futures",

      swingHighs:
        structure.swingHighs,

      swingLows:
        structure.swingLows,

      equalHighs:
        structure.equalHighs,

      equalLows:
        structure.equalLows,

      majorBSL:
        structure.majorBSL.map(
          formatLevel
        ),

      majorSSL:
        structure.majorSSL.map(
          formatLevel
        ),

      majorResistance:
        structure.majorResistance.map(
          formatLevel
        ),

      majorSupport:
        structure.majorSupport.map(
          formatLevel
        ),

      updatedAt:
        structure.updatedAt
    });
  }
);


/* =========================================================
   LIQUIDITY ENDPOINT
========================================================= */

app.get(
  "/liquidity",
  (req, res) => {

    const map =
      getLiquidityMap();

    res.json({

      ok: true,

      symbol:
        SYMBOL,

      currentPrice,

      marketStructure: {

        buySideLiquidity:
          map.buySideLiquidity
            .map(formatLevel),

        sellSideLiquidity:
          map.sellSideLiquidity
            .map(formatLevel),

        majorResistance:
          map.majorResistance
            .map(formatLevel),

        majorSupport:
          map.majorSupport
            .map(formatLevel),

        nearestBSL:
          map.nearestBSL
            .map(formatLevel),

        nearestSSL:
          map.nearestSSL
            .map(formatLevel)
      },

      rawRestingLiquidity:
        getRawRestingLiquidity(),

      structureStats: {

        candleSource,

        candleHistory: {
          "15m":
            candleHistory["15m"].length,

          "1h":
            candleHistory["1h"].length,

          "4h":
            candleHistory["4h"].length,

          "1d":
            candleHistory["1d"].length
        },

        swingHighs:
          structure.swingHighs.length,

        swingLows:
          structure.swingLows.length,

        equalHighs:
          structure.equalHighs.length,

        equalLows:
          structure.equalLows.length
      },

      updatedAt:
        now()
    });
  }
);


/* =========================================================
   BOOK ENDPOINT
========================================================= */

app.get(
  "/book",
  (req, res) => {

    const bidArray =
      Array.from(
        bids.entries()
      )
      .map(
        ([price, quantity]) => ({
          price:
            Number(price),

          quantity:
            Number(quantity),

          usd:
            Number(price) *
            Number(quantity)
        })
      )
      .sort(
        (a, b) =>
          b.price - a.price
      )
      .slice(
        0,
        1000
      );

    const askArray =
      Array.from(
        asks.entries()
      )
      .map(
        ([price, quantity]) => ({
          price:
            Number(price),

          quantity:
            Number(quantity),

          usd:
            Number(price) *
            Number(quantity)
        })
      )
      .sort(
        (a, b) =>
          a.price - b.price
      )
      .slice(
        0,
        1000
      );

    res.json({

      ok: true,

      symbol:
        SYMBOL,

      currentPrice,

      initialized,

      lastUpdateId:
        depthLastUpdateId,

      bids:
        bidArray,

      asks:
        askArray,

      updatedAt:
        now()
    });
  }
);


/* =========================================================
   ROOT
========================================================= */

app.get(
  "/",
  (req, res) => {

    res.json({

      ok: true,

      service:
        "Binance BTCUSDT Major Liquidity Relay",

      endpoints: [

        "/health",

        "/structure",

        "/liquidity",

        "/book"
      ],

      architecture: {

        historical:
          "Binance Spot",

        liveStructure:
          "Binance Futures WebSocket",

        liquidity:
          "Binance Futures Order Book"
      },

      symbol:
        SYMBOL,

      updatedAt:
        now()
    });
  }
);


/* =========================================================
   START
========================================================= */

app.listen(
  PORT,
  async () => {

    console.log(
      `Server listening on port ${PORT}`
    );

    console.log(
      `[START] Symbol: ${SYMBOL}`
    );

    console.log(
      "[START] Loading historical Spot candles..."
    );

    await loadAllHistoricalCandles();

    console.log(
      "[START] Connecting Futures depth..."
    );

    connectDepth();

    console.log(
      "[START] Connecting Futures klines..."
    );

    connectFuturesKlines();
  }
);
