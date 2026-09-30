const express = require("express");
const WebSocket = require("ws");
const axios = require("axios");

const app = express();

const PORT = process.env.PORT || 10000;
const SYMBOL = "BTCUSDT";
const LOWER = SYMBOL.toLowerCase();

/* =========================================================
   DATA SOURCES
========================================================= */

const SPOT_KLINES =
  "https://data-api.binance.vision/api/v3/klines";

const FUTURES_DEPTH_WS =
  `wss://fstream.binance.com/ws/${LOWER}@depth@100ms`;

const FUTURES_KLINE_WS =
  `wss://fstream.binance.com/stream?streams=` +
  `${LOWER}@kline_15m/` +
  `${LOWER}@kline_1h/` +
  `${LOWER}@kline_4h/` +
  `${LOWER}@kline_1d`;

const SNAPSHOT_WS =
  "wss://ws-fapi.binance.com/ws-fapi/v1";

/* =========================================================
   STRUCTURE SETTINGS
========================================================= */

const STRUCTURE = {
  "15m": {
    interval: "15m",
    limit: 300,
    weight: 1,
    left: 3,
    right: 3,
    equalTolerance: 0.0015
  },

  "1h": {
    interval: "1h",
    limit: 300,
    weight: 2,
    left: 4,
    right: 4,
    equalTolerance: 0.0015
  },

  "4h": {
    interval: "4h",
    limit: 250,
    weight: 3,
    left: 5,
    right: 5,
    equalTolerance: 0.002
  },

  "1d": {
    interval: "1d",
    limit: 180,
    weight: 4,
    left: 5,
    right: 5,
    equalTolerance: 0.0025
  }
};

/* =========================================================
   LIQUIDITY SETTINGS
========================================================= */

const RAW_BOOK_MIN_USD = 50000;

const RESTING_RADIUS = 35;

const MIN_RESTING_CONFIRMATION = 250000;

const MERGE_DISTANCE = 35;

const MIN_MAJOR_SCORE = 40;

const MAX_MAJOR_LEVELS = 8;

const REACTION_MOVE = 0.003;

/* =========================================================
   ORDER BOOK
========================================================= */

const bids = new Map();
const asks = new Map();

let orderBookInitialized = false;
let depthLastUpdateId = 0;

let currentPrice = null;

let depthWs = null;
let klineWs = null;
let snapshotWs = null;

let depthConnected = false;
let klineConnected = false;
let snapshotConnected = false;

let pendingDepthEvents = [];

let gapRecovery = false;
let gapDetected = false;
let gapCount = 0;

let lastGapLocal = null;
let lastGapIncoming = null;

/* =========================================================
   SNAPSHOT CONTROL
========================================================= */

let snapshotPending = false;
let snapshotRequests = 0;
let snapshot429s = 0;

let lastSnapshotRequest = 0;
let lastSnapshotResponse = null;
let snapshotLastStatus = null;
let snapshotLastError = null;

const SNAPSHOT_COOLDOWN = 65000;

/* =========================================================
   CANDLES
========================================================= */

const candles = {
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
   STRUCTURE STATE
========================================================= */

const structure = {
  swingHighs: [],
  swingLows: [],
  equalHighs: [],
  equalLows: [],
  majorBSL: [],
  majorSSL: [],
  majorResistance: [],
  majorSupport: [],
  updatedAt: null
};

/* =========================================================
   HELPERS
========================================================= */

function now() {
  return new Date().toISOString();
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function pctDistance(a, b) {
  if (!a || !b) {
    return 999;
  }

  return Math.abs(a - b) / b;
}

function money(value) {
  return Number(value.toFixed(2));
}

/* =========================================================
   SPOT HISTORICAL CANDLES
========================================================= */

async function loadHistoricalCandles(tf) {
  const cfg = STRUCTURE[tf];

  candleRequests[tf]++;

  try {
    const response = await axios.get(SPOT_KLINES, {
      params: {
        symbol: SYMBOL,
        interval: cfg.interval,
        limit: cfg.limit
      },

      timeout: 15000,

      headers: {
        "User-Agent": "Mozilla/5.0"
      }
    });

    const result = [];

    for (const row of response.data || []) {
      if (!Array.isArray(row) || row.length < 6) {
        continue;
      }

      result.push({
        openTime: Number(row[0]),
        open: Number(row[1]),
        high: Number(row[2]),
        low: Number(row[3]),
        close: Number(row[4]),
        volume: Number(row[5]),
        closeTime: Number(row[6] || 0),
        source: "spot"
      });
    }

    if (result.length === 0) {
      throw new Error("No candles returned");
    }

    candles[tf] = result;
    candleSource[tf] = "binance_spot";

    candleSuccess[tf]++;
    candleLastStatus = response.status;
    candleLastError = null;

    console.log(
      `[HISTORY] ${tf}: ${result.length} Spot candles`
    );

    return true;

  } catch (error) {
    candleErrors[tf]++;

    candleLastStatus =
      error.response?.status || null;

    candleLastError =
      error.response?.data
        ? JSON.stringify(error.response.data)
        : error.message;

    console.log(
      `[HISTORY ERROR] ${tf}:`,
      candleLastStatus,
      candleLastError
    );

    return false;
  }
}

/* =========================================================
   LOAD ALL HISTORY
========================================================= */

async function loadHistory() {
  console.log("[HISTORY] Loading Spot candles...");

  for (const tf of Object.keys(STRUCTURE)) {
    await loadHistoricalCandles(tf);
    await sleep(300);
  }

  rebuildStructure();

  console.log("[HISTORY] Structure initialized");
}

/* =========================================================
   FUTURES LIVE KLINES
========================================================= */

function connectKlines() {
  if (klineWs) {
    try {
      klineWs.close();
    } catch (_) {}
  }

  console.log("[KLINE] Connecting...");

  klineWs = new WebSocket(FUTURES_KLINE_WS);

  klineWs.on("open", () => {
    klineConnected = true;

    console.log("[KLINE] Connected");
  });

  klineWs.on("message", raw => {
    try {
      const message = JSON.parse(raw.toString());

      const data = message.data || message;

      if (data.e !== "kline") {
        return;
      }

      const k = data.k;

      if (!k) {
        return;
      }

      const tf = k.i;

      if (!STRUCTURE[tf]) {
        return;
      }

      const candle = {
        openTime: Number(k.t),
        open: Number(k.o),
        high: Number(k.h),
        low: Number(k.l),
        close: Number(k.c),
        volume: Number(k.v),
        closeTime: Number(k.T || 0),
        source: "futures"
      };

      const list = candles[tf];

      const existing =
        list.findIndex(
          item => item.openTime === candle.openTime
        );

      if (existing >= 0) {
        list[existing] = candle;
      } else {
        list.push(candle);
        list.sort(
          (a, b) => a.openTime - b.openTime
        );
      }

      const max =
        STRUCTURE[tf].limit + 50;

      if (list.length > max) {
        candles[tf] =
          list.slice(list.length - max);
      }

      candleSource[tf] =
        "binance_futures_live";

      currentPrice = candle.close;

      if (k.x === true) {
        rebuildStructure();
      }

    } catch (error) {
      console.log(
        "[KLINE MESSAGE ERROR]",
        error.message
      );
    }
  });

  klineWs.on("error", error => {
    console.log(
      "[KLINE ERROR]",
      error.message
    );
  });

  klineWs.on("close", () => {
    klineConnected = false;

    console.log("[KLINE] Disconnected");

    setTimeout(
      connectKlines,
      5000
    );
  });
}

/* =========================================================
   PIVOTS
========================================================= */

function pivotHigh(list, index, left, right) {
  if (
    index - left < 0 ||
    index + right >= list.length
  ) {
    return false;
  }

  const value = list[index].high;

  for (
    let i = index - left;
    i <= index + right;
    i++
  ) {
    if (i === index) {
      continue;
    }

    if (list[i].high >= value) {
      return false;
    }
  }

  return true;
}

function pivotLow(list, index, left, right) {
  if (
    index - left < 0 ||
    index + right >= list.length
  ) {
    return false;
  }

  const value = list[index].low;

  for (
    let i = index - left;
    i <= index + right;
    i++
  ) {
    if (i === index) {
      continue;
    }

    if (list[i].low <= value) {
      return false;
    }
  }

  return true;
}

/* =========================================================
   EXTRACT SWINGS
========================================================= */

function extractSwings(tf) {
  const cfg = STRUCTURE[tf];
  const list = candles[tf];

  const highs = [];
  const lows = [];

  if (
    list.length <
    cfg.left + cfg.right + 5
  ) {
    return {
      highs,
      lows
    };
  }

  for (
    let i = cfg.left;
    i < list.length - cfg.right;
    i++
  ) {
    if (
      pivotHigh(
        list,
        i,
        cfg.left,
        cfg.right
      )
    ) {
      highs.push({
        timeframe: tf,
        price: list[i].high,
        time: list[i].openTime,
        type: "swing_high"
      });
    }

    if (
      pivotLow(
        list,
        i,
        cfg.left,
        cfg.right
      )
    ) {
      lows.push({
        timeframe: tf,
        price: list[i].low,
        time: list[i].openTime,
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
   MERGE STRUCTURAL LEVELS
========================================================= */

function mergeLevels(levels, side) {
  if (!levels.length) {
    return [];
  }

  const sorted =
    [...levels].sort(
      (a, b) => a.price - b.price
    );

  const groups = [];

  for (const item of sorted) {
    let group = null;

    for (const existing of groups) {
      if (
        Math.abs(
          item.price - existing.price
        ) <= MERGE_DISTANCE
      ) {
        group = existing;
        break;
      }
    }

    if (!group) {
      group = {
        side,
        prices: [],
        timeframes: new Set()
      };

      groups.push(group);
    }

    group.prices.push(item.price);
    group.timeframes.add(item.timeframe);
  }

  return groups.map(group => {
    const price =
      group.prices.reduce(
        (a, b) => a + b,
        0
      ) /
      group.prices.length;

    return {
      side,
      price,
      priceLow: Math.min(...group.prices),
      priceHigh: Math.max(...group.prices),
      touches: group.prices.length,
      timeframes:
        [...group.timeframes]
    };
  });
}

/* =========================================================
   EQUAL LEVELS
========================================================= */

function calculateEqualLevels(levels, tolerance) {
  const result = [];

  for (let i = 0; i < levels.length; i++) {
    const matches = [levels[i]];

    for (let j = 0; j < levels.length; j++) {
      if (i === j) {
        continue;
      }

      if (
        pctDistance(
          levels[i].price,
          levels[j].price
        ) <= tolerance
      ) {
        matches.push(levels[j]);
      }
    }

    if (matches.length >= 2) {
      const average =
        matches.reduce(
          (sum, item) =>
            sum + item.price,
          0
        ) / matches.length;

      const exists =
        result.some(
          item =>
            pctDistance(
              item.price,
              average
            ) < 0.0005
        );

      if (!exists) {
        result.push({
          price: average,
          count: matches.length,
          timeframes:
            [...new Set(
              matches.map(
                item => item.timeframe
              )
            )]
        });
      }
    }
  }

  return result;
}

/* =========================================================
   REACTION COUNT
========================================================= */

function reactionCount(price, tf) {
  const list = candles[tf];

  if (!list.length) {
    return 0;
  }

  const zone = price * 0.0015;

  let reactions = 0;

  for (
    let i = 0;
    i < list.length - 4;
    i++
  ) {
    const candle = list[i];

    const touched =
      candle.high >= price - zone &&
      candle.low <= price + zone;

    if (!touched) {
      continue;
    }

    let highest = candle.high;
    let lowest = candle.low;

    for (
      let j = i + 1;
      j <= i + 4;
      j++
    ) {
      highest =
        Math.max(
          highest,
          list[j].high
        );

      lowest =
        Math.min(
          lowest,
          list[j].low
        );
    }

    const upMove =
      (highest - price) / price;

    const downMove =
      (price - lowest) / price;

    if (
      upMove >= REACTION_MOVE ||
      downMove >= REACTION_MOVE
    ) {
      reactions++;
    }
  }

  return reactions;
}

/* =========================================================
   RESTING LIQUIDITY
========================================================= */

function restingNear(price, side) {
  const source =
    side === "BSL"
      ? asks
      : bids;

  let totalUsd = 0;
  const levels = [];

  for (const [priceText, quantity] of source) {
    const levelPrice =
      Number(priceText);

    const qty =
      Number(quantity);

    if (
      !Number.isFinite(levelPrice) ||
      !Number.isFinite(qty)
    ) {
      continue;
    }

    if (
      Math.abs(
        levelPrice - price
      ) > RESTING_RADIUS
    ) {
      continue;
    }

    const usd =
      levelPrice * qty;

    if (
      usd < RAW_BOOK_MIN_USD
    ) {
      continue;
    }

    totalUsd += usd;

    levels.push({
      price: levelPrice,
      quantity: qty,
      usd
    });
  }

  levels.sort(
    (a, b) => b.usd - a.usd
  );

  return {
    totalUsd,
    levels: levels.slice(0, 10)
  };
}

/* =========================================================
   SCORE STRUCTURAL LEVEL
========================================================= */

function scoreLevel(level, side, equalLevels) {
  let score = 0;

  for (const tf of level.timeframes) {
    score +=
      (STRUCTURE[tf]?.weight || 0) * 10;
  }

  if (level.timeframes.length >= 2) {
    score += 10;
  }

  if (level.timeframes.length >= 3) {
    score += 10;
  }

  score += Math.min(
    level.touches * 3,
    15
  );

  let reactions = 0;

  for (const tf of level.timeframes) {
    reactions +=
      reactionCount(
        level.price,
        tf
      );
  }

  score += Math.min(
    reactions * 2,
    15
  );

  let equalCount = 0;

  for (const equal of equalLevels) {
    if (
      pctDistance(
        equal.price,
        level.price
      ) <= 0.0015
    ) {
      equalCount += equal.count;
    }
  }

  score += Math.min(
    equalCount * 6,
    18
  );

  const resting =
    restingNear(
      level.price,
      side
    );

  if (
    resting.totalUsd >=
    MIN_RESTING_CONFIRMATION
  ) {
    score += 15;
  } else if (
    resting.totalUsd >= 100000
  ) {
    score += 7;
  }

  return {
    ...level,
    score: Math.round(score),
    reactions,
    equalCount,
    restingUsd: resting.totalUsd,
    restingLevels: resting.levels,
    source:
      "spot_structure+futures_liquidity"
  };
}

/* =========================================================
   REBUILD STRUCTURE
========================================================= */

function rebuildStructure() {
  const allHighs = [];
  const allLows = [];

  const equalHighs = [];
  const equalLows = [];

  for (const tf of Object.keys(STRUCTURE)) {
    const result =
      extractSwings(tf);

    allHighs.push(...result.highs);
    allLows.push(...result.lows);

    const eqHigh =
      calculateEqualLevels(
        result.highs,
        STRUCTURE[tf].equalTolerance
      );

    const eqLow =
      calculateEqualLevels(
        result.lows,
        STRUCTURE[tf].equalTolerance
      );

    for (const item of eqHigh) {
      equalHighs.push({
        ...item,
        timeframe: tf
      });
    }

    for (const item of eqLow) {
      equalLows.push({
        ...item,
        timeframe: tf
      });
    }
  }

  structure.swingHighs = allHighs;
  structure.swingLows = allLows;
  structure.equalHighs = equalHighs;
  structure.equalLows = equalLows;

  const bslGroups =
    mergeLevels(
      allHighs,
      "BSL"
    );

  const sslGroups =
    mergeLevels(
      allLows,
      "SSL"
    );

  structure.majorBSL =
    bslGroups
      .map(level =>
        scoreLevel(
          level,
          "BSL",
          equalHighs
        )
      )
      .filter(
        level =>
          level.score >= MIN_MAJOR_SCORE
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
    sslGroups
      .map(level =>
        scoreLevel(
          level,
          "SSL",
          equalLows
        )
      )
      .filter(
        level =>
          level.score >= MIN_MAJOR_SCORE
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
      .filter(
        level =>
          currentPrice === null ||
          level.price > currentPrice
      )
      .sort(
        (a, b) =>
          a.price - b.price
      )
      .slice(0, 3);

  structure.majorSupport =
    structure.majorSSL
      .filter(
        level =>
          currentPrice === null ||
          level.price < currentPrice
      )
      .sort(
        (a, b) =>
          b.price - a.price
      )
      .slice(0, 3);

  structure.updatedAt = now();
}

/* =========================================================
   DEPTH UPDATE
========================================================= */

function applyDepthUpdate(data) {
  if (!data) {
    return;
  }

  const U = Number(data.U);
  const u = Number(data.u);

  if (
    !Number.isFinite(U) ||
    !Number.isFinite(u)
  ) {
    return;
  }

  if (!orderBookInitialized) {
    pendingDepthEvents.push(data);

    if (pendingDepthEvents.length > 20000) {
      pendingDepthEvents =
        pendingDepthEvents.slice(-20000);
    }

    return;
  }

  const expected =
    depthLastUpdateId + 1;

  if (U > expected) {
    lastGapLocal =
      depthLastUpdateId;

    lastGapIncoming = U;

    gapDetected = true;

    if (!gapRecovery) {
      gapRecovery = true;
      gapCount++;

      console.log(
        "[DEPTH] Gap detected",
        expected,
        U
      );
    }

    return;
  }

  if (u <= depthLastUpdateId) {
    return;
  }

  for (const item of data.b || []) {
    const price = Number(item[0]);
    const quantity = Number(item[1]);

    if (
      !Number.isFinite(price) ||
      !Number.isFinite(quantity)
    ) {
      continue;
    }

    if (quantity === 0) {
      bids.delete(item[0]);
    } else {
      bids.set(
        item[0],
        quantity
      );
    }
  }

  for (const item of data.a || []) {
    const price = Number(item[0]);
    const quantity = Number(item[1]);

    if (
      !Number.isFinite(price) ||
      !Number.isFinite(quantity)
    ) {
      continue;
    }

    if (quantity === 0) {
      asks.delete(item[0]);
    } else {
      asks.set(
        item[0],
        quantity
      );
    }
  }

  depthLastUpdateId = u;

  updatePrice();
}

/* =========================================================
   PRICE
========================================================= */

function updatePrice() {
  let bestBid = null;
  let bestAsk = null;

  for (const priceText of bids.keys()) {
    const price = Number(priceText);

    if (
      bestBid === null ||
      price > bestBid
    ) {
      bestBid = price;
    }
  }

  for (const priceText of asks.keys()) {
    const price = Number(priceText);

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
    currentPrice =
      (bestBid + bestAsk) / 2;
  }
}

/* =========================================================
   SNAPSHOT
========================================================= */

function requestSnapshot() {
  const time = Date.now();

  if (snapshotPending) {
    return;
  }

  if (
    time - lastSnapshotRequest <
    SNAPSHOT_COOLDOWN
  ) {
    return;
  }

  lastSnapshotRequest = time;
  snapshotPending = true;
  snapshotRequests++;

  console.log(
    "[SNAPSHOT] Requesting..."
  );

  try {
    if (snapshotWs) {
      try {
        snapshotWs.close();
      } catch (_) {}
    }

    snapshotWs =
      new WebSocket(SNAPSHOT_WS);

    snapshotWs.on("open", () => {
      snapshotConnected = true;

      snapshotWs.send(
        JSON.stringify({
          id: String(Date.now()),
          method: "depth",
          params: {
            symbol: SYMBOL,
            limit: 1000
          }
        })
      );
    });

    snapshotWs.on("message", raw => {
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

          snapshotPending = false;

          return;
        }

        const result =
          response.result;

        if (
          !result ||
          !result.lastUpdateId
        ) {
          return;
        }

        bids.clear();
        asks.clear();

        for (const item of result.bids || []) {
          const price = Number(item[0]);
          const quantity = Number(item[1]);

          if (
            price > 0 &&
            quantity > 0
          ) {
            bids.set(
              item[0],
              quantity
            );
          }
        }

        for (const item of result.asks || []) {
          const price = Number(item[0]);
          const quantity = Number(item[1]);

          if (
            price > 0 &&
            quantity > 0
          ) {
            asks.set(
              item[0],
              quantity
            );
          }
        }

        depthLastUpdateId =
          Number(
            result.lastUpdateId
          );

        orderBookInitialized = true;

        snapshotLastStatus = 200;
        snapshotLastError = null;
        lastSnapshotResponse = now();

        /*
        Apply buffered events after the
        snapshot where possible.
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

        const target =
          depthLastUpdateId + 1;

        let bridge = -1;

        for (
          let i = 0;
          i < buffered.length;
          i++
        ) {
          const U =
            Number(buffered[i].U);

          const u =
            Number(buffered[i].u);

          if (
            U <= target &&
            u >= target
          ) {
            bridge = i;
            break;
          }
        }

        if (bridge >= 0) {
          for (
            let i = bridge;
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
          gapRecovery = true;
          gapDetected = true;
        }

        snapshotPending = false;

        updatePrice();

        console.log(
          "[SNAPSHOT] Installed",
          depthLastUpdateId,
          "bids:",
          bids.size,
          "asks:",
          asks.size
        );

      } catch (error) {
        snapshotLastError =
          error.message;

        snapshotPending = false;
      }
    });

    snapshotWs.on("error", error => {
      snapshotLastError =
        error.message;

      snapshotPending = false;

      console.log(
        "[SNAPSHOT ERROR]",
        error.message
      );
    });

    snapshotWs.on("close", () => {
      snapshotConnected = false;
    });

  } catch (error) {
    snapshotLastError =
      error.message;

    snapshotPending = false;
  }
}

/* =========================================================
   DEPTH CONNECTION
========================================================= */

function connectDepth() {
  if (depthWs) {
    try {
      depthWs.close();
    } catch (_) {}
  }

  console.log("[DEPTH] Connecting...");

  depthWs =
    new WebSocket(
      FUTURES_DEPTH_WS
    );

  depthWs.on("open", () => {
    depthConnected = true;

    console.log(
      "[DEPTH] Binance Futures connected"
    );

    requestSnapshot();
  });

  depthWs.on("message", raw => {
    try {
      const data =
        JSON.parse(
          raw.toString()
        );

      if (
        data.e === "depthUpdate"
      ) {
        applyDepthUpdate(data);
      }
    } catch (error) {
      console.log(
        "[DEPTH MESSAGE ERROR]",
        error.message
      );
    }
  });

  depthWs.on("error", error => {
    console.log(
      "[DEPTH ERROR]",
      error.message
    );
  });

  depthWs.on("close", () => {
    depthConnected = false;

    console.log(
      "[DEPTH] Disconnected"
    );

    setTimeout(
      connectDepth,
      5000
    );
  });
}

/* =========================================================
   RAW BOOK
========================================================= */

function rawLiquidity() {
  const largeBids = [];
  const largeAsks = [];

  for (const [priceText, quantity] of bids) {
    const price = Number(priceText);
    const usd = price * Number(quantity);

    if (usd >= RAW_BOOK_MIN_USD) {
      largeBids.push({
        price,
        quantity: Number(quantity),
        usd
      });
    }
  }

  for (const [priceText, quantity] of asks) {
    const price = Number(priceText);
    const usd = price * Number(quantity);

    if (usd >= RAW_BOOK_MIN_USD) {
      largeAsks.push({
        price,
        quantity: Number(quantity),
        usd
      });
    }
  }

  largeBids.sort(
    (a, b) => b.usd - a.usd
  );

  largeAsks.sort(
    (a, b) => b.usd - a.usd
  );

  return {
    largeBids:
      largeBids.slice(0, 25),

    largeAsks:
      largeAsks.slice(0, 25)
  };
}

/* =========================================================
   FORMAT STRUCTURAL LEVEL
========================================================= */

function formatLevel(level) {
  return {
    side: level.side,

    price: money(level.price),

    priceLow:
      money(level.priceLow),

    priceHigh:
      money(level.priceHigh),

    score:
      level.score,

    timeframes:
      level.timeframes,

    equalCount:
      level.equalCount,

    reactions:
      level.reactions,

    restingUsd:
      money(level.restingUsd),

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
   LIQUIDITY MAP
========================================================= */

function liquidityMap() {
  const bsl =
    structure.majorBSL
      .filter(
        level =>
          currentPrice === null ||
          level.price > currentPrice
      )
      .sort(
        (a, b) =>
          a.price - b.price
      );

  const ssl =
    structure.majorSSL
      .filter(
        level =>
          currentPrice === null ||
          level.price < currentPrice
      )
      .sort(
        (a, b) =>
          b.price - a.price
      );

  return {
    buySideLiquidity:
      bsl.map(formatLevel),

    sellSideLiquidity:
      ssl.map(formatLevel),

    majorResistance:
      bsl
        .slice(0, 3)
        .map(formatLevel),

    majorSupport:
      ssl
        .slice(0, 3)
        .map(formatLevel),

    nearestBSL:
      bsl.length
        ? [formatLevel(bsl[0])]
        : [],

    nearestSSL:
      ssl.length
        ? [formatLevel(ssl[0])]
        : []
  };
}

/* =========================================================
   HEALTH
========================================================= */

app.get("/health", (req, res) => {
  res.json({
    ok: true,

    service:
      "Binance BTCUSDT Major Liquidity Relay",

    symbol: SYMBOL,

    currentPrice,

    historicalStructureSource:
      "Binance Spot",

    liveStructureSource:
      "Binance Futures WebSocket",

    liquiditySource:
      "Binance Futures Order Book",

    candleHistory: {
      "15m": candles["15m"].length,
      "1h": candles["1h"].length,
      "4h": candles["4h"].length,
      "1d": candles["1d"].length
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
      initialized:
        orderBookInitialized,

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
        klineConnected
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

      snapshotId:
        depthLastUpdateId,

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

    updatedAt: now()
  });
});

/* =========================================================
   STRUCTURE ENDPOINT
========================================================= */

app.get("/structure", (req, res) => {
  res.json({
    ok: true,

    symbol: SYMBOL,

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
});

/* =========================================================
   LIQUIDITY ENDPOINT
========================================================= */

app.get("/liquidity", (req, res) => {
  const map =
    liquidityMap();

  res.json({
    ok: true,

    symbol: SYMBOL,

    currentPrice,

    marketStructure: map,

    rawRestingLiquidity:
      rawLiquidity(),

    structureStats: {
      candleSource,

      candleHistory: {
        "15m": candles["15m"].length,
        "1h": candles["1h"].length,
        "4h": candles["4h"].length,
        "1d": candles["1d"].length
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

    updatedAt: now()
  });
});

/* =========================================================
   BOOK ENDPOINT
========================================================= */

app.get("/book", (req, res) => {
  const bidList =
    [...bids.entries()]
      .map(
        ([price, quantity]) => ({
          price: Number(price),
          quantity: Number(quantity),
          usd:
            Number(price) *
            Number(quantity)
        })
      )
      .sort(
        (a, b) =>
          b.price - a.price
      )
      .slice(0, 1000);

  const askList =
    [...asks.entries()]
      .map(
        ([price, quantity]) => ({
          price: Number(price),
          quantity: Number(quantity),
          usd:
            Number(price) *
            Number(quantity)
        })
      )
      .sort(
        (a, b) =>
          a.price - b.price
      )
      .slice(0, 1000);

  res.json({
    ok: true,

    symbol: SYMBOL,

    currentPrice,

    initialized:
      orderBookInitialized,

    lastUpdateId:
      depthLastUpdateId,

    bids: bidList,

    asks: askList,

    updatedAt: now()
  });
});

/* =========================================================
   ROOT
========================================================= */

app.get("/", (req, res) => {
  res.json({
    ok: true,

    service:
      "Binance BTCUSDT Major Liquidity Relay",

    symbol: SYMBOL,

    endpoints: [
      "/health",
      "/structure",
      "/liquidity",
      "/book"
    ]
  });
});

/* =========================================================
   RECOVERY LOOP
========================================================= */

setInterval(() => {
  if (gapRecovery) {
    requestSnapshot();
  }
}, 5000);

/* =========================================================
   STRUCTURE REFRESH
========================================================= */

setInterval(() => {
  rebuildStructure();
}, 30000);

/* =========================================================
   START SERVER
========================================================= */

app.listen(PORT, async () => {
  console.log(
    `Server listening on port ${PORT}`
  );

  console.log(
    "[START] Loading historical candles..."
  );

  await loadHistory();

  console.log(
    "[START] Starting Futures depth..."
  );

  connectDepth();

  console.log(
    "[START] Starting Futures klines..."
  );

  connectKlines();
});
