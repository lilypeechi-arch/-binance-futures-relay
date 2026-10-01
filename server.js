/* ============================================================
   BINANCE BTCUSDT LIQUIDITY / MARKET STRUCTURE RELAY
   V11 - HEALTH + STABLE SNAPSHOT + STRUCTURE CACHE

   DATA
   ------------------------------------------------------------
   Historical structure : Binance Spot
   Live order book       : Binance Futures depth WS
   Live candles          : Binance Futures kline WS
   Snapshot              : Binance Futures WS API

   IMPORTANT
   ------------------------------------------------------------
   BSL = structural highs ABOVE current price
   SSL = structural lows BELOW current price

   Resting bids/asks are separate from structural liquidity.
   ============================================================ */

const express = require("express");
const WebSocket = require("ws");

const app = express();
app.use(express.json());

const PORT = Number(process.env.PORT || 10000);
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

const FUTURES_API_WS =
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
  maxLevels: 12,

  bookBucketUsd: 5,
  orderBucketMinUsd: 250000,
  orderConcentrationMultiple: 2.5,
  orderMinUsd: 750000,
  orderSingleBucketMinUsd: 1000000,
  orderGiantWallUsd: 1500000,
  minStrongBuckets: 2,
  orderMaxClusterWidthUsd: 50,
  orderSplitRatio: 0.45,
  orderMaxStrongBucketsPerCluster: 6,

  persistencePollMs: 5000,
  persistenceConfirmMs: 60000,
  persistenceMaxAgeMs: 10 * 60 * 1000,
  persistenceTolerancePct: 0.0005,
  persistenceRemovedMinObservations: 2,
  persistenceRemovedMinLifetimeMs: 10000,

  eventMax: 100,

  sweepLookbackMs: 24 * 60 * 60 * 1000,
  sweepMinPenetrationPct: 0.00015,

  majorMinDistancePct: 0.50,
  majorMinimumTimeframeWeight: 2,
  major1hMinScore: 90,
  majorHigherTfMinScore: 40,

  structureCacheMaxAgeMs: 30 * 60 * 1000,

  confluenceMaxDistancePct: 0.001,
  confluenceMaxDistanceUsd: 75,
  confluenceMinOverlapRatio: 0.20,

  snapshotCooldownMs: 5000,
  maxPendingDepthEvents: 5000,

  depthReconnectMs: 3000,
  snapshotReconnectMs: 3000,
  klineReconnectMs: 3000,

  requestTimeoutMs: 15000
};

/* ============================================================
   STRUCTURE
   ============================================================ */

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
    eq: 0.002
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
   STATE
   ============================================================ */

const state = {
  symbol: SYMBOL,

  currentPrice: null,
  priceSource: null,

  bids: new Map(),
  asks: new Map(),

  initialized: false,
  lastUpdateId: 0,

  pendingDepthEvents: [],

  waitingForBridge: true,
  snapshotPending: false,
  snapshotInFlight: false,

  syncGeneration: 0,

  lastSnapshotRequestAt: 0,
  lastSnapshotId: null,
  lastBridgeU: null,

  lastGapExpected: null,
  lastGapReceived: null,

  depthWs: null,
  depthConnected: false,

  snapshotWs: null,
  snapshotConnected: false,

  klineWs: null,
  klineConnected: false,

  snapshotRequests: 0,
  snapshot429s: 0,
  bridgeAttempts: 0,
  bridgeFound: 0,
  resyncs: 0,
  sequenceGaps: 0,

  candles: {
    "15m": [],
    "1h": [],
    "4h": [],
    "1d": []
  },

  structure: {
    swingHighs: [],
    swingLows: [],

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

    retainedPrevious: false,

    lastGoodLevels: [],
    lastGoodMajorLevels: [],

    lastGoodAt: null
  },

  persistence: {
    bids: [],
    asks: [],
    removed: []
  },

  liquidityEvents: [],
  recentSweeps: [],

  lastError: null,

  startedAt: new Date().toISOString(),
  updatedAt: null
};

/* ============================================================
   BASIC
   ============================================================ */

function now() {
  return Date.now();
}

function round(value, decimals = 2) {
  if (!Number.isFinite(value)) return null;

  const factor = 10 ** decimals;

  return Math.round(value * factor) / factor;
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function absPct(a, b) {
  if (
    !Number.isFinite(a) ||
    !Number.isFinite(b) ||
    b === 0
  ) {
    return Infinity;
  }

  return Math.abs(a - b) / Math.abs(b);
}

function distancePct(price, currentPrice) {
  if (
    !Number.isFinite(price) ||
    !Number.isFinite(currentPrice) ||
    currentPrice === 0
  ) {
    return 0;
  }

  return ((price - currentPrice) / currentPrice) * 100;
}

function median(values) {
  const arr = values
    .filter(Number.isFinite)
    .slice()
    .sort((a, b) => a - b);

  if (!arr.length) return 0;

  const m = Math.floor(arr.length / 2);

  return arr.length % 2
    ? arr[m]
    : (arr[m - 1] + arr[m]) / 2;
}

function weightedMid(items) {
  if (!items.length) return null;

  let totalUsd = 0;
  let totalValue = 0;

  for (const item of items) {
    const usd = Number(item.usd) || 0;
    const price = Number(item.price) || 0;

    totalUsd += usd;
    totalValue += price * usd;
  }

  return totalUsd > 0
    ? totalValue / totalUsd
    : null;
}

/* ============================================================
   PRICE
   ============================================================ */

function updateCurrentPrice() {
  const bids = [...state.bids.keys()]
    .map(Number)
    .filter(Number.isFinite)
    .sort((a, b) => b - a);

  const asks = [...state.asks.keys()]
    .map(Number)
    .filter(Number.isFinite)
    .sort((a, b) => a - b);

  if (bids.length && asks.length) {
    state.currentPrice =
      (bids[0] + asks[0]) / 2;

    state.priceSource =
      "futures_orderbook_mid";
  }

  return state.currentPrice;
}

/* ============================================================
   FETCH
   ============================================================ */

async function fetchJson(url) {
  const controller = new AbortController();

  const timer = setTimeout(
    () => controller.abort(),
    SETTINGS.requestTimeoutMs
  );

  try {
    const response = await fetch(url, {
      headers: {
        "User-Agent":
          "BTCUSDT-Liquidity-Relay/11.0"
      },
      signal: controller.signal
    });

    if (!response.ok) {
      const body = await response.text();

      throw new Error(
        `HTTP ${response.status}: ${body.slice(0, 300)}`
      );
    }

    return await response.json();
  } finally {
    clearTimeout(timer);
  }
}

/* ============================================================
   HISTORICAL CANDLES
   ============================================================ */

async function loadHistoricalCandles(tf) {
  const config = STRUCTURE[tf];

  const url =
    `${SPOT_API}/api/v3/klines` +
    `?symbol=${SYMBOL}` +
    `&interval=${config.interval}` +
    `&limit=${config.candles}`;

  try {
    const data = await fetchJson(url);

    const candles = data
      .map(row => ({
        time: Number(row[0]),
        open: Number(row[1]),
        high: Number(row[2]),
        low: Number(row[3]),
        close: Number(row[4]),
        volume: Number(row[5]),
        closeTime: Number(row[6]),
        closed: true
      }))
      .filter(
        c =>
          Number.isFinite(c.high) &&
          Number.isFinite(c.low) &&
          Number.isFinite(c.close)
      );

    state.candles[tf] = candles;

    console.log(
      `Historical ${tf}: ${candles.length}`
    );
  } catch (error) {
    state.lastError =
      `Historical ${tf}: ${error.message}`;

    console.error(
      `Historical ${tf} failed`,
      error.message
    );
  }
}

async function loadAllHistoricalCandles() {
  await Promise.all(
    Object.keys(STRUCTURE).map(
      loadHistoricalCandles
    )
  );

  rebuildStructure();
}

/* ============================================================
   PIVOTS
   ============================================================ */

function detectPivots(candles, left, right) {
  const highs = [];
  const lows = [];

  if (
    !candles ||
    candles.length <
      left + right + 5
  ) {
    return { highs, lows };
  }

  for (
    let i = left;
    i < candles.length - right;
    i++
  ) {
    const candle = candles[i];

    let high = true;
    let low = true;

    for (
      let j = i - left;
      j <= i + right;
      j++
    ) {
      if (j === i) continue;

      if (candles[j].high >= candle.high) {
        high = false;
      }

      if (candles[j].low <= candle.low) {
        low = false;
      }

      if (!high && !low) break;
    }

    if (high) {
      highs.push({
        price: candle.high,
        time: candle.time
      });
    }

    if (low) {
      lows.push({
        price: candle.low,
        time: candle.time
      });
    }
  }

  return { highs, lows };
}

/* ============================================================
   CLUSTER PIVOTS
   ============================================================ */

function clusterPivots(pivots, tolerancePct) {
  const clusters = [];

  const sorted = pivots
    .slice()
    .sort((a, b) => a.price - b.price);

  for (const pivot of sorted) {
    let best = null;
    let bestDistance = Infinity;

    for (const cluster of clusters) {
      const d = absPct(
        pivot.price,
        cluster.price
      );

      if (
        d <= tolerancePct &&
        d < bestDistance
      ) {
        best = cluster;
        bestDistance = d;
      }
    }

    if (!best) {
      clusters.push({
        price: pivot.price,
        priceLow: pivot.price,
        priceHigh: pivot.price,
        members: [pivot],
        times: [pivot.time]
      });
    } else {
      best.members.push(pivot);
      best.times.push(pivot.time);

      best.priceLow =
        Math.min(
          best.priceLow,
          pivot.price
        );

      best.priceHigh =
        Math.max(
          best.priceHigh,
          pivot.price
        );

      best.price =
        best.members.reduce(
          (sum, x) => sum + x.price,
          0
        ) / best.members.length;
    }
  }

  return clusters;
}

/* ============================================================
   REACTIONS
   ============================================================ */

function countReactions(
  candles,
  cluster,
  side
) {
  let reactions = 0;

  const low =
    cluster.priceLow *
    (1 - SETTINGS.reactionPct);

  const high =
    cluster.priceHigh *
    (1 + SETTINGS.reactionPct);

  for (
    let i = 0;
    i < candles.length;
    i++
  ) {
    const candle = candles[i];

    const touched =
      side === "BSL"
        ? candle.high >= low &&
          candle.high <= high
        : candle.low >= low &&
          candle.low <= high;

    if (!touched) continue;

    const end =
      Math.min(
        candles.length - 1,
        i + 8
      );

    for (
      let j = i + 1;
      j <= end;
      j++
    ) {
      const future = candles[j];

      if (
        side === "BSL" &&
        future.low <=
          cluster.price *
            (1 - SETTINGS.reactionPct)
      ) {
        reactions++;
        break;
      }

      if (
        side === "SSL" &&
        future.high >=
          cluster.price *
            (1 + SETTINGS.reactionPct)
      ) {
        reactions++;
        break;
      }
    }
  }

  return reactions;
}

/* ============================================================
   TF STRUCTURE
   ============================================================ */

function buildTFStructure(tf) {
  const config = STRUCTURE[tf];
  const candles = state.candles[tf];

  if (
    !config ||
    !candles ||
    candles.length < 20
  ) {
    return {
      highs: [],
      lows: []
    };
  }

  const pivots =
    detectPivots(
      candles,
      config.left,
      config.right
    );

  const highClusters =
    clusterPivots(
      pivots.highs,
      config.eq
    );

  const lowClusters =
    clusterPivots(
      pivots.lows,
      config.eq
    );

  const highs =
    highClusters.map(c => ({
      side: "BSL",
      price: c.price,
      priceLow: c.priceLow,
      priceHigh: c.priceHigh,
      equalCount: c.members.length,
      reactions:
        countReactions(
          candles,
          c,
          "BSL"
        ),
      timeframe: tf,
      timeframeWeight: config.weight
    }));

  const lows =
    lowClusters.map(c => ({
      side: "SSL",
      price: c.price,
      priceLow: c.priceLow,
      priceHigh: c.priceHigh,
      equalCount: c.members.length,
      reactions:
        countReactions(
          candles,
          c,
          "SSL"
        ),
      timeframe: tf,
      timeframeWeight: config.weight
    }));

  return { highs, lows };
}

/* ============================================================
   MERGE STRUCTURE
   ============================================================ */

function mergeStructureLevels(levels) {
  const merged = [];

  const sorted = levels
    .slice()
    .sort((a, b) => a.price - b.price);

  for (const level of sorted) {
    let best = null;
    let bestDistance = Infinity;

    for (const existing of merged) {
      if (
        existing.side !== level.side
      ) {
        continue;
      }

      const distance =
        absPct(
          level.price,
          existing.price
        );

      const tolerance =
        Math.max(
          SETTINGS.zonePct,
          SETTINGS.mergeUsd /
            Math.max(
              level.price,
              1
            )
        );

      if (
        distance <= tolerance &&
        distance < bestDistance
      ) {
        best = existing;
        bestDistance = distance;
      }
    }

    if (!best) {
      merged.push({
        side: level.side,
        price: level.price,
        priceLow: level.priceLow,
        priceHigh: level.priceHigh,
        equalCount: level.equalCount,
        reactions: level.reactions,
        timeframes: [level.timeframe],
        timeframeWeight:
          level.timeframeWeight,
        scoreBase: 0,
        score: 0,
        restingUsd: 0,
        restingConfirmed: false,
        restingLevels: []
      });
    } else {
      best.price =
        (best.price + level.price) / 2;

      best.priceLow =
        Math.min(
          best.priceLow,
          level.priceLow
        );

      best.priceHigh =
        Math.max(
          best.priceHigh,
          level.priceHigh
        );

      best.equalCount +=
        level.equalCount;

      best.reactions +=
        level.reactions;

      if (
        !best.timeframes.includes(
          level.timeframe
        )
      ) {
        best.timeframes.push(
          level.timeframe
        );
      }

      best.timeframeWeight +=
        level.timeframeWeight;
    }
  }

  return merged;
}

/* ============================================================
   SCORE
   ============================================================ */

function scoreStructureLevel(level) {
  const score =
    level.timeframeWeight * 10 +
    Math.min(level.equalCount, 30) * 5 +
    Math.min(level.reactions, 40) * 2;

  level.scoreBase = score;
  level.score = score;

  if (
    score >= 125 ||
    (
      level.timeframes.length >= 3 &&
      level.equalCount >= 4
    )
  ) {
    level.strength = "HIGH";
  } else if (
    score >= 90 ||
    level.timeframes.length >= 2 ||
    level.equalCount >= 3
  ) {
    level.strength = "MEDIUM";
  } else {
    level.strength = "LOW";
  }

  return level;
}

/* ============================================================
   DIRECTION
   ============================================================ */

function isDirectionalLevel(
  level,
  currentPrice
) {
  if (!Number.isFinite(currentPrice)) {
    return false;
  }

  if (
    level.side === "BSL"
  ) {
    return level.price > currentPrice;
  }

  if (
    level.side === "SSL"
  ) {
    return level.price < currentPrice;
  }

  return false;
}

/* ============================================================
   MAJOR
   ============================================================ */

function isMajorLevel(level) {
  const price = state.currentPrice;

  if (!Number.isFinite(price)) {
    return false;
  }

  if (
    !isDirectionalLevel(
      level,
      price
    )
  ) {
    return false;
  }

  const distance =
    Math.abs(
      level.price - price
    ) / price;

  if (
    distance <
    SETTINGS.majorMinDistancePct / 100
  ) {
    return false;
  }

  if (
    level.timeframeWeight <
    SETTINGS.majorMinimumTimeframeWeight
  ) {
    return false;
  }

  if (
    level.timeframes.length === 1 &&
    level.timeframes.includes("1h") &&
    level.score <
      SETTINGS.major1hMinScore
  ) {
    return false;
  }

  if (
    level.timeframes.length === 1 &&
    (
      level.timeframes.includes("4h") ||
      level.timeframes.includes("1d")
    ) &&
    level.score <
      SETTINGS.majorHigherTfMinScore
  ) {
    return false;
  }

  return true;
}

/* ============================================================
   NEAR
   ============================================================ */

function isNearLevel(level) {
  const price = state.currentPrice;

  if (!Number.isFinite(price)) {
    return false;
  }

  if (
    !isDirectionalLevel(
      level,
      price
    )
  ) {
    return false;
  }

  return (
    Math.abs(
      level.price - price
    ) / price < 0.005
  );
}

/* ============================================================
   RESTING LIQUIDITY
   ============================================================ */

function getRestingLiquidity(level) {
  const map =
    level.side === "BSL"
      ? state.asks
      : state.bids;

  const rows = [];

  for (
    const [
      priceString,
      quantity
    ] of map.entries()
  ) {
    const price =
      Number(priceString);

    const qty =
      Number(quantity);

    if (
      !Number.isFinite(price) ||
      !Number.isFinite(qty) ||
      qty <= 0
    ) {
      continue;
    }

    const usd = price * qty;

    if (
      usd <
      SETTINGS.rawMinUsd
    ) {
      continue;
    }

    if (
      Math.abs(
        price - level.price
      ) <=
      SETTINGS.restingRadiusUsd
    ) {
      rows.push({
        price,
        quantity: qty,
        usd
      });
    }
  }

  rows.sort(
    (a, b) => b.usd - a.usd
  );

  /*
     Do not expose hundreds of raw levels.
  */

  const top =
    rows.slice(0, 10);

  const usd =
    top.reduce(
      (sum, x) => sum + x.usd,
      0
    );

  level.restingUsd = usd;

  level.restingConfirmed =
    usd >=
    SETTINGS.restingConfirmUsd;

  level.restingLevels = top;

  level.score =
    level.scoreBase +
    clamp(
      usd / 100000,
      0,
      25
    );

  if (level.score >= 125) {
    level.strength = "HIGH";
  } else if (level.score >= 90) {
    level.strength = "MEDIUM";
  } else {
    level.strength = "LOW";
  }

  return level;
}

/* ============================================================
   FILTER OLD CACHE
   ============================================================ */

function filterCachedLevel(
  level,
  currentPrice
) {
  if (
    !level ||
    !Number.isFinite(level.price) ||
    !Number.isFinite(currentPrice)
  ) {
    return false;
  }

  /*
     Preserve only levels still on the correct
     side of current price.
  */

  if (
    level.side === "BSL" &&
    level.price <= currentPrice
  ) {
    return false;
  }

  if (
    level.side === "SSL" &&
    level.price >= currentPrice
  ) {
    return false;
  }

  return true;
}

/* ============================================================
   REBUILD STRUCTURE
   ============================================================ */

function rebuildStructure() {
  const s = state.structure;

  s.lastAttemptAt =
    new Date().toISOString();

  s.rebuildCount++;

  const highs = [];
  const lows = [];

  for (
    const tf of Object.keys(STRUCTURE)
  ) {
    const result =
      buildTFStructure(tf);

    highs.push(...result.highs);
    lows.push(...result.lows);
  }

  const merged =
    mergeStructureLevels([
      ...highs,
      ...lows
    ])
      .map(scoreStructureLevel);

  const directional =
    merged.filter(level =>
      isDirectionalLevel(
        level,
        state.currentPrice
      )
    );

  const candidateMajor =
    directional.filter(
      isMajorLevel
    );

  s.lastCandidateLevels =
    directional.length;

  s.lastCandidateMajor =
    candidateMajor.length;

  /*
     Temporary empty candidate:
     DO NOT ERASE a valid previous structure.
  */

  if (
    directional.length === 0
  ) {
    s.rejectedRebuilds++;
    s.lastRebuildAccepted = false;

    const age =
      s.lastGoodAt
        ? now() -
          new Date(
            s.lastGoodAt
          ).getTime()
        : Infinity;

    const cached =
      s.lastGoodLevels.filter(
        level =>
          filterCachedLevel(
            level,
            state.currentPrice
          )
      );

    const cachedMajor =
      s.lastGoodMajorLevels.filter(
        level =>
          filterCachedLevel(
            level,
            state.currentPrice
          ) &&
          isMajorLevel(level)
      );

    if (
      cached.length &&
      age <
        SETTINGS.structureCacheMaxAgeMs
    ) {
      s.levels =
        cached.map(
          x => ({ ...x })
        );

      s.majorLevels =
        cachedMajor.map(
          x => ({ ...x })
        );

      s.nearLevels =
        s.levels
          .filter(isNearLevel);

      s.retainedPrevious = true;

      s.status =
        "STALE_FALLBACK";

      s.lastRebuildReason =
        "retained_previous_major_structure";

      return;
    }

    s.levels = [];
    s.majorLevels = [];
    s.nearLevels = [];

    s.retainedPrevious = false;

    s.status =
      "NO_STRUCTURE";

    s.lastRebuildReason =
      "rejected_empty_directional_structure";

    return;
  }

  /*
     Candidate exists.
     Accept it even if there are temporarily
     zero major levels.
  */

  const accepted =
    directional.map(
      getRestingLiquidity
    );

  s.levels = accepted;

  s.majorLevels =
    accepted
      .filter(isMajorLevel)
      .sort(
        (a, b) =>
          Math.abs(
            a.price -
              state.currentPrice
          ) -
          Math.abs(
            b.price -
              state.currentPrice
          )
      );

  s.nearLevels =
    accepted
      .filter(isNearLevel)
      .sort(
        (a, b) =>
          Math.abs(
            a.price -
              state.currentPrice
          ) -
          Math.abs(
            b.price -
              state.currentPrice
          )
      );

  s.lastGoodLevels =
    s.levels.map(
      x => ({ ...x })
    );

  s.lastGoodMajorLevels =
    s.majorLevels.map(
      x => ({ ...x })
    );

  s.lastGoodAt =
    new Date().toISOString();

  s.lastGoodRebuildAt =
    s.lastGoodAt;

  s.lastRebuildAccepted = true;

  s.retainedPrevious = false;

  s.lastRebuildReason =
    s.majorLevels.length
      ? "accepted_with_major_structure"
      : "accepted_without_major_structure";

  s.status =
    s.majorLevels.length
      ? "HEALTHY"
      : "HEALTHY_NO_MAJOR";
}

/* ============================================================
   STRUCTURE GETTERS
   ============================================================ */

function getMajorLevels(side) {
  return state.structure.majorLevels
    .filter(
      level =>
        level.side === side &&
        isDirectionalLevel(
          level,
          state.currentPrice
        )
    )
    .sort((a, b) =>
      side === "BSL"
        ? a.price - b.price
        : b.price - a.price
    );
}

function getNearLevels(side) {
  return state.structure.nearLevels
    .filter(
      level =>
        level.side === side &&
        isDirectionalLevel(
          level,
          state.currentPrice
        )
    )
    .sort(
      (a, b) =>
        Math.abs(
          a.price -
            state.currentPrice
        ) -
        Math.abs(
          b.price -
            state.currentPrice
        )
    );
}

/* ============================================================
   BOOK ROWS
   ============================================================ */

function getBookRows(side) {
  const map =
    side === "bid"
      ? state.bids
      : state.asks;

  const rows = [];

  for (
    const [
      priceString,
      quantity
    ] of map.entries()
  ) {
    const price =
      Number(priceString);

    const qty =
      Number(quantity);

    if (
      !Number.isFinite(price) ||
      !Number.isFinite(qty) ||
      qty <= 0
    ) {
      continue;
    }

    const usd =
      price * qty;

    if (
      usd <
      SETTINGS.rawMinUsd
    ) {
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
   BOOK BUCKETS
   ============================================================ */

function makeBookBuckets(side) {
  const rows =
    getBookRows(side);

  const size =
    SETTINGS.bookBucketUsd;

  const map = new Map();

  for (const row of rows) {
    const low =
      Math.floor(
        row.price / size
      ) * size;

    const key =
      low.toFixed(2);

    let bucket =
      map.get(key);

    if (!bucket) {
      bucket = {
        priceLow: low,
        priceHigh: low + size,
        center: low + size / 2,
        usd: 0,
        rawLevels: 0,
        strongestPrice: row.price,
        strongestUsd: row.usd
      };

      map.set(key, bucket);
    }

    bucket.usd += row.usd;
    bucket.rawLevels++;

    if (
      row.usd >
      bucket.strongestUsd
    ) {
      bucket.strongestUsd =
        row.usd;

      bucket.strongestPrice =
        row.price;
    }
  }

  return [...map.values()]
    .sort(
      (a, b) =>
        a.priceLow -
        b.priceLow
    );
}

/* ============================================================
   BOOK CLUSTERS
   ============================================================ */

function buildBookClusters(side) {
  const buckets =
    makeBookBuckets(side);

  if (!buckets.length) {
    return [];
  }

  const medianUsd =
    median(
      buckets.map(
        x => x.usd
      )
    );

  const threshold =
    Math.max(
      SETTINGS.orderBucketMinUsd,
      medianUsd *
        SETTINGS.orderConcentrationMultiple
    );

  const strong =
    buckets.map(
      bucket => ({
        ...bucket,
        strong:
          bucket.usd >= threshold,
        giant:
          bucket.usd >=
          SETTINGS.orderGiantWallUsd
      })
    );

  const output = [];
  let run = [];

  function emit(segment) {
    if (!segment.length) return;

    const total =
      segment.reduce(
        (sum, x) =>
          sum + x.usd,
        0
      );

    const giant =
      segment.some(
        x => x.giant
      );

    if (
      segment.length <
        SETTINGS.minStrongBuckets &&
      !giant &&
      total <
        SETTINGS.orderSingleBucketMinUsd
    ) {
      return;
    }

    if (
      total <
        SETTINGS.orderMinUsd &&
      !giant
    ) {
      return;
    }

    const low =
      segment[0].priceLow;

    const high =
      segment[
        segment.length - 1
      ].priceHigh;

    const width =
      high - low;

    if (
      width >
      SETTINGS.orderMaxClusterWidthUsd
    ) {
      return;
    }

    let strongest =
      segment[0];

    for (const b of segment) {
      if (
        b.usd >
        strongest.usd
      ) {
        strongest = b;
      }
    }

    /*
       Calculate concentration against
       ALL buckets in the same physical span.
    */

    const allInSpan =
      buckets.filter(
        b =>
          b.priceHigh > low &&
          b.priceLow < high
      );

    const allUsd =
      allInSpan.reduce(
        (sum, b) =>
          sum + b.usd,
        0
      );

    const concentrationRatio =
      allUsd > 0
        ? total / allUsd
        : 0;

    const midpoint =
      weightedMid(
        segment.map(
          b => ({
            price: b.center,
            usd: b.usd
          })
        )
      );

    output.push({
      side,

      priceLow: low,
      priceHigh: high,

      midpoint:
        midpoint ||
        (low + high) / 2,

      usd: total,

      levels:
        segment.length,

      rawLevels:
        segment.reduce(
          (sum, b) =>
            sum + b.rawLevels,
          0
        ),

      strongBuckets:
        segment.length,

      strongestPrice:
        strongest.strongestPrice,

      strongestBucketUsd:
        strongest.usd,

      strongestRawOrderUsd:
        strongest.strongestUsd,

      medianBucketUsd: medianUsd,

      concentrationThreshold:
        threshold,

      concentrationRatio,

      width,

      giantWall: giant
    });
  }

  function flush() {
    if (run.length) {
      emit(run);
    }

    run = [];
  }

  for (const bucket of strong) {
    if (!bucket.strong) {
      flush();
      continue;
    }

    if (!run.length) {
      run = [bucket];
      continue;
    }

    const previous =
      run[run.length - 1];

    const gap =
      bucket.priceLow -
      previous.priceHigh;

    const width =
      bucket.priceHigh -
      run[0].priceLow;

    if (
      gap >
        SETTINGS.bookBucketUsd * 1.5 ||
      width >
        SETTINGS.orderMaxClusterWidthUsd ||
      bucket.usd <
        previous.usd *
          SETTINGS.orderSplitRatio ||
      run.length >=
        SETTINGS.orderMaxStrongBucketsPerCluster
    ) {
      flush();
      run = [bucket];
      continue;
    }

    run.push(bucket);
  }

  flush();

  return output
    .sort(
      (a, b) =>
        b.usd - a.usd
    )
    .slice(0, 20);
}

/* ============================================================
   CONFLUENCE
   ============================================================ */

function findStructuralConfluence(cluster) {
  const major =
    state.structure.majorLevels
      .filter(isMajorLevel);

  let best = null;

  for (const level of major) {
    if (
      level.side === "BSL" &&
      cluster.side !== "ask"
    ) {
      continue;
    }

    if (
      level.side === "SSL" &&
      cluster.side !== "bid"
    ) {
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
        overlapHigh -
          overlapLow
      );

    const clusterWidth =
      Math.max(
        1,
        cluster.priceHigh -
          cluster.priceLow
      );

    const overlapRatio =
      overlap /
      clusterWidth;

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

    if (
      !strongestInside &&
      overlapRatio <
        SETTINGS.confluenceMinOverlapRatio &&
      distance > allowed
    ) {
      continue;
    }

    const quality =
      (strongestInside ? 100 : 0) +
      overlapRatio * 50 +
      Math.max(
        0,
        1 -
          distance /
            Math.max(
              allowed,
              1
            )
      ) *
        25 +
      level.score / 20;

    if (
      !best ||
      quality > best.quality
    ) {
      best = {
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
          overlap > 0,
        structuralOverlapRatio:
          overlapRatio,
        structuralDistance:
          distance,
        quality
      };
    }
  }

  return (
    best || {
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
    }
  );
}

/* ============================================================
   ORDER BOOK INTELLIGENCE
   ============================================================ */

function buildOrderBookIntelligence() {
  const bids =
    buildBookClusters("bid");

  const asks =
    buildBookClusters("ask");

  for (const x of bids) {
    Object.assign(
      x,
      findStructuralConfluence(x)
    );
  }

  for (const x of asks) {
    Object.assign(
      x,
      findStructuralConfluence(x)
    );
  }

  function sortWalls(list) {
    return list
      .sort((a, b) => {
        const sa =
          a.usd *
          (
            1 +
            Math.min(
              a.concentrationRatio,
              1
            ) *
              0.5
          );

        const sb =
          b.usd *
          (
            1 +
            Math.min(
              b.concentrationRatio,
              1
            ) *
              0.5
          );

        return sb - sa;
      })
      .slice(0, 8);
  }

  return {
    bids: sortWalls(bids),
    asks: sortWalls(asks)
  };
}

/* ============================================================
   PERSISTENCE
   ============================================================ */

function clustersMatch(a, b) {
  if (
    !a ||
    !b ||
    a.side !== b.side
  ) {
    return false;
  }

  const overlap =
    Math.min(
      a.priceHigh,
      b.priceHigh
    ) -
    Math.max(
      a.priceLow,
      b.priceLow
    );

  if (overlap >= 0) {
    return true;
  }

  const tolerance =
    Math.max(
      20,
      (state.currentPrice || 0) *
        SETTINGS.persistenceTolerancePct
    );

  return (
    Math.abs(
      a.midpoint -
        b.midpoint
    ) <= tolerance
  );
}

function addLiquidityEvent(event) {
  state.liquidityEvents.unshift({
    ...event,
    eventTime:
      new Date().toISOString()
  });

  state.liquidityEvents =
    state.liquidityEvents.slice(
      0,
      SETTINGS.eventMax
    );
}

function updatePersistence() {
  if (!state.initialized) return;

  const book =
    buildOrderBookIntelligence();

  const timestamp = now();

  for (const side of ["bid", "ask"]) {
    const current =
      side === "bid"
        ? book.bids
        : book.asks;

    const key =
      side === "bid"
        ? "bids"
        : "asks";

    const previous =
      state.persistence[key];

    const matched =
      new Set();

    const next = [];

    for (const cluster of current) {
      let index = -1;

      for (
        let i = 0;
        i < previous.length;
        i++
      ) {
        if (matched.has(i)) continue;

        if (
          clustersMatch(
            previous[i],
            cluster
          )
        ) {
          index = i;
          break;
        }
      }

      if (index >= 0) {
        matched.add(index);

        const old =
          previous[index];

        const previousUsd =
          Number(old.usd) || 0;

        const currentUsd =
          Number(cluster.usd) || 0;

        const firstSeen =
          old.firstSeen ||
          timestamp;

        const duration =
          timestamp -
          firstSeen;

        const observations =
          (old.observations || 0) +
          1;

        const item = {
          ...cluster,

          firstSeen,
          lastSeen: timestamp,

          durationMs: duration,

          durationSec:
            Math.floor(
              duration / 1000
            ),

          observations,

          maxUsd:
            Math.max(
              old.maxUsd || 0,
              currentUsd
            ),

          previousUsd,

          changeUsd:
            currentUsd -
            previousUsd,

          status:
            duration >=
            SETTINGS.persistenceConfirmMs
              ? "HOLDING"
              : "BUILDING",

          persistent:
            duration >=
            SETTINGS.persistenceConfirmMs,

          persistentEventSent:
            !!old.persistentEventSent,

          confluenceEventSent:
            !!old.confluenceEventSent,

          strengthEventAt:
            old.strengthEventAt || 0,

          removed: false
        };

        Object.assign(
          item,
          findStructuralConfluence(item)
        );

        if (
          item.persistent &&
          !item.persistentEventSent
        ) {
          addLiquidityEvent({
            type: "PERSISTENT",
            side: item.side,
            midpoint: item.midpoint,
            priceLow: item.priceLow,
            priceHigh: item.priceHigh,
            usd: item.usd,
            durationSec:
              item.durationSec,
            strongestPrice:
              item.strongestPrice,
            strongestBucketUsd:
              item.strongestBucketUsd
          });

          item.persistentEventSent =
            true;
        }

        if (
          item.persistent &&
          item.structuralConfluence &&
          !item.confluenceEventSent
        ) {
          addLiquidityEvent({
            type: "CONFLUENCE",
            side: item.side,
            midpoint: item.midpoint,
            usd: item.usd,
            structuralLevel:
              item.structuralLevel,
            structuralZoneLow:
              item.structuralZoneLow,
            structuralZoneHigh:
              item.structuralZoneHigh,
            structuralTimeframes:
              item.structuralTimeframes
          });

          item.confluenceEventSent =
            true;
        }

        const changePct =
          previousUsd > 0
            ? Math.abs(
                currentUsd -
                  previousUsd
              ) /
              previousUsd
            : 0;

        if (
          item.persistent &&
          changePct >= 0.10 &&
          timestamp -
            (old.strengthEventAt || 0) >=
            15000
        ) {
          addLiquidityEvent({
            type:
              currentUsd >
              previousUsd
                ? "STRENGTHENING"
                : "WEAKENING",
            side: item.side,
            midpoint: item.midpoint,
            usd: item.usd,
            previousUsd,
            changeUsd:
              currentUsd -
              previousUsd,
            changePct:
              changePct * 100
          });

          item.strengthEventAt =
            timestamp;
        }

        next.push(item);
      } else {
        next.push({
          ...cluster,

          ...findStructuralConfluence(
            cluster
          ),

          firstSeen: timestamp,
          lastSeen: timestamp,

          durationMs: 0,
          durationSec: 0,

          observations: 1,

          maxUsd: cluster.usd,
          previousUsd: 0,
          changeUsd: cluster.usd,

          status: "NEW",
          persistent: false,

          persistentEventSent: false,
          confluenceEventSent: false,
          strengthEventAt: 0,

          removed: false
        });
      }
    }

    for (
      let i = 0;
      i < previous.length;
      i++
    ) {
      if (matched.has(i)) continue;

      const old =
        previous[i];

      const lifetime =
        timestamp -
        (old.firstSeen || timestamp);

      const observations =
        old.observations || 0;

      const meaningful =
        observations >=
          SETTINGS.persistenceRemovedMinObservations ||
        lifetime >=
          SETTINGS.persistenceRemovedMinLifetimeMs;

      if (
        meaningful &&
        !old.removedEventSent
      ) {
        addLiquidityEvent({
          type: "REMOVED",
          side: old.side,
          midpoint: old.midpoint,
          priceLow: old.priceLow,
          priceHigh: old.priceHigh,
          usd: old.usd,
          lifetimeSec:
            Math.floor(
              lifetime / 1000
            ),
          observations,
          persistent:
            !!old.persistent
        });

        old.removedEventSent = true;
      }

      if (meaningful) {
        state.persistence.removed.push({
          ...old,
          removed: true,
          removedAt: timestamp
        });
      }
    }

    state.persistence[key] = next;
  }

  state.persistence.removed =
    state.persistence.removed.filter(
      x =>
        timestamp -
          (x.removedAt || timestamp) <
        SETTINGS.persistenceMaxAgeMs
    );

  state.updatedAt =
    new Date().toISOString();
}

/* ============================================================
   SWEEPS
   ============================================================ */

function registerSweep({
  side,
  level,
  tf,
  candle,
  sweepPrice
}) {
  const duplicate =
    state.recentSweeps.find(
      x =>
        x.side === side &&
        x.level === level.price &&
        x.timeframe === tf &&
        x.candleTime === candle.time
    );

  if (duplicate) return;

  state.recentSweeps.unshift({
    side,
    level: level.price,
    zoneLow: level.priceLow,
    zoneHigh: level.priceHigh,
    sweepPrice,
    timeframe: tf,
    candleTime: candle.time,
    status: "SWEPT",
    reclaimed: true,
    detectedAt:
      new Date().toISOString(),
    timeframes:
      level.timeframes
  });

  state.recentSweeps =
    state.recentSweeps.slice(
      0,
      50
    );
}

function detectCandleSweeps(
  tf,
  candle
) {
  if (!candle) return;

  if (
    now() -
      candle.closeTime >
    SETTINGS.sweepLookbackMs
  ) {
    return;
  }

  const levels =
    state.structure.majorLevels.slice();

  for (const level of levels) {
    if (
      level.side === "BSL"
    ) {
      const penetration =
        level.price *
        SETTINGS.sweepMinPenetrationPct;

      if (
        candle.high >=
          level.priceHigh +
            penetration &&
        candle.close <
          level.priceHigh
      ) {
        registerSweep({
          side: "BSL",
          level,
          tf,
          candle,
          sweepPrice:
            candle.high
        });
      }
    }

    if (
      level.side === "SSL"
    ) {
      const penetration =
        level.price *
        SETTINGS.sweepMinPenetrationPct;

      if (
        candle.low <=
          level.priceLow -
            penetration &&
        candle.close >
          level.priceLow
      ) {
        registerSweep({
          side: "SSL",
          level,
          tf,
          candle,
          sweepPrice:
            candle.low
        });
      }
    }
  }
}

/* ============================================================
   CANDLE UPDATE
   ============================================================ */

function upsertCandle(
  tf,
  candle
) {
  const candles =
    state.candles[tf];

  if (!candles) return;

  const index =
    candles.findIndex(
      x =>
        x.time === candle.time
    );

  if (index >= 0) {
    candles[index] = candle;
  } else {
    candles.push(candle);
  }

  const max =
    STRUCTURE[tf].candles + 20;

  if (candles.length > max) {
    state.candles[tf] =
      candles.slice(-max);
  }

  if (candle.closed) {
    /*
       Detect sweep against previous
       structure BEFORE rebuilding.
    */
    detectCandleSweeps(
      tf,
      candle
    );

    rebuildStructure();
  }
}

/* ============================================================
   KLINE WS
   ============================================================ */

function connectKlineWs() {
  if (
    state.klineWs &&
    (
      state.klineWs.readyState ===
        WebSocket.OPEN ||
      state.klineWs.readyState ===
        WebSocket.CONNECTING
    )
  ) {
    return;
  }

  const ws =
    new WebSocket(KLINE_WS);

  state.klineWs = ws;

  ws.on("open", () => {
    state.klineConnected = true;

    console.log(
      "Kline websocket connected"
    );
  });

  ws.on("message", raw => {
    try {
      const msg =
        JSON.parse(
          raw.toString()
        );

      const data = msg.data;

      if (
        !data ||
        data.e !== "kline"
      ) {
        return;
      }

      const k = data.k;
      const tf = k.i;

      if (!STRUCTURE[tf]) return;

      upsertCandle(tf, {
        time: Number(k.t),
        open: Number(k.o),
        high: Number(k.h),
        low: Number(k.l),
        close: Number(k.c),
        volume: Number(k.v),
        closeTime: Number(k.T),
        closed: !!k.x
      });

      updateCurrentPrice();
    } catch (error) {
      state.lastError =
        `Kline parse: ${error.message}`;
    }
  });

  ws.on("error", error => {
    state.lastError =
      `Kline WS: ${error.message}`;

    console.error(
      "Kline WS:",
      error.message
    );
  });

  ws.on("close", () => {
    state.klineConnected = false;

    setTimeout(
      connectKlineWs,
      SETTINGS.klineReconnectMs
    );
  });
}

/* ============================================================
   DIRECT BOOK APPLY
   ============================================================ */

function applyDepthEventDirect(event) {
  for (const item of event.b || []) {
    const price = Number(item[0]);
    const quantity = Number(item[1]);

    if (
      !Number.isFinite(price) ||
      !Number.isFinite(quantity)
    ) {
      continue;
    }

    const key = String(price);

    if (quantity === 0) {
      state.bids.delete(key);
    } else {
      state.bids.set(
        key,
        quantity
      );
    }
  }

  for (const item of event.a || []) {
    const price = Number(item[0]);
    const quantity = Number(item[1]);

    if (
      !Number.isFinite(price) ||
      !Number.isFinite(quantity)
    ) {
      continue;
    }

    const key = String(price);

    if (quantity === 0) {
      state.asks.delete(key);
    } else {
      state.asks.set(
        key,
        quantity
      );
    }
  }

  state.lastUpdateId =
    Number(event.u);

  updateCurrentPrice();
}

/* ============================================================
   SNAPSHOT REQUEST
   ============================================================ */

function requestSnapshot(force = false) {
  const timestamp = now();

  if (
    state.snapshotInFlight
  ) {
    return;
  }

  if (
    !force &&
    timestamp -
      state.lastSnapshotRequestAt <
      SETTINGS.snapshotCooldownMs
  ) {
    return;
  }

  if (
    !state.snapshotWs ||
    state.snapshotWs.readyState !==
      WebSocket.OPEN
  ) {
    connectSnapshotWs();
    return;
  }

  state.lastSnapshotRequestAt =
    timestamp;

  state.snapshotPending = true;
  state.waitingForBridge = true;
  state.snapshotInFlight = true;

  state.snapshotRequests++;

  /*
     Each snapshot belongs to one generation.
     Old responses cannot overwrite newer sync attempts.
  */

  const generation =
    ++state.syncGeneration;

  state.activeSnapshotGeneration =
    generation;

  const id =
    `depth-${timestamp}-${generation}`;

  const request = {
    id,
    method: "depth",
    params: {
      symbol: SYMBOL,
      limit: 1000
    }
  };

  try {
    state.snapshotWs.send(
      JSON.stringify(request)
    );
  } catch (error) {
    state.snapshotInFlight = false;

    state.lastError =
      `Snapshot send: ${error.message}`;
  }
}

/* ============================================================
   SNAPSHOT PROCESS
   ============================================================ */

function processSnapshot(result) {
  state.snapshotInFlight = false;

  if (
    !result ||
    !Number.isFinite(
      Number(result.lastUpdateId)
    )
  ) {
    state.lastError =
      "Invalid Futures depth snapshot";

    return;
  }

  const snapshotId =
    Number(result.lastUpdateId);

  state.lastSnapshotId =
    snapshotId;

  state.bridgeAttempts++;

  /*
     Find event spanning snapshotId + 1.
  */

  const target =
    snapshotId + 1;

  let bridgeIndex = -1;

  for (
    let i = 0;
    i < state.pendingDepthEvents.length;
    i++
  ) {
    const event =
      state.pendingDepthEvents[i];

    const U = Number(event.U);
    const u = Number(event.u);

    if (
      U <= target &&
      u >= target
    ) {
      bridgeIndex = i;
      break;
    }
  }

  if (bridgeIndex < 0) {
    state.waitingForBridge = true;
    state.snapshotPending = true;

    /*
       If the buffer has moved far beyond the snapshot,
       request a fresh snapshot after cooldown.
    */

    const newest =
      state.pendingDepthEvents[
        state.pendingDepthEvents.length - 1
      ];

    if (
      newest &&
      Number(newest.u) >
        snapshotId + 10000
    ) {
      setTimeout(
        () => requestSnapshot(true),
        SETTINGS.snapshotCooldownMs
      );
    }

    return;
  }

  state.bridgeFound++;

  /*
     Build book from snapshot.
  */

  state.bids.clear();
  state.asks.clear();

  for (const item of result.bids || []) {
    const price = Number(item[0]);
    const quantity = Number(item[1]);

    if (
      Number.isFinite(price) &&
      Number.isFinite(quantity) &&
      quantity > 0
    ) {
      state.bids.set(
        String(price),
        quantity
      );
    }
  }

  for (const item of result.asks || []) {
    const price = Number(item[0]);
    const quantity = Number(item[1]);

    if (
      Number.isFinite(price) &&
      Number.isFinite(quantity) &&
      quantity > 0
    ) {
      state.asks.set(
        String(price),
        quantity
      );
    }
  }

  /*
     Apply bridge exactly ONCE.
  */

  const bridge =
    state.pendingDepthEvents[
      bridgeIndex
    ];

  applyDepthEventDirect(
    bridge
  );

  state.lastBridgeU =
    Number(bridge.u);

  /*
     Everything after bridge.
  */

  const remaining =
    state.pendingDepthEvents.slice(
      bridgeIndex + 1
    );

  state.pendingDepthEvents = [];

  /*
     Replay without calling the normal
     gap-resync function.
  */

  for (const event of remaining) {
    const U = Number(event.U);
    const u = Number(event.u);

    const expected =
      state.lastUpdateId + 1;

    if (u < expected) {
      continue;
    }

    if (U > expected) {
      state.sequenceGaps++;

      state.lastGapExpected =
        expected;

      state.lastGapReceived =
        U;

      state.initialized = false;
      state.waitingForBridge = true;
      state.snapshotPending = true;

      /*
         Keep the current book.
         Schedule one fresh sync.
      */

      setTimeout(
        () => requestSnapshot(true),
        SETTINGS.snapshotCooldownMs
      );

      return;
    }

    applyDepthEventDirect(
      event
    );
  }

  /*
     SUCCESS.
  */

  state.initialized = true;
  state.waitingForBridge = false;
  state.snapshotPending = false;

  updateCurrentPrice();

  state.updatedAt =
    new Date().toISOString();

  console.log(
    `Depth LIVE at ${state.lastUpdateId}`
  );
}

/* ============================================================
   NORMAL DEPTH EVENT
   ============================================================ */

function processLiveDepthEvent(event) {
  const U = Number(event.U);
  const u = Number(event.u);

  if (
    !Number.isFinite(U) ||
    !Number.isFinite(u)
  ) {
    return;
  }

  const expected =
    state.lastUpdateId + 1;

  /*
     Old/duplicate event.
  */

  if (u < expected) {
    return;
  }

  /*
     Correct overlapping event.
  */

  if (
    U <= expected &&
    u >= expected
  ) {
    applyDepthEventDirect(event);
    return;
  }

  /*
     Genuine gap.
  */

  state.sequenceGaps++;

  state.lastGapExpected =
    expected;

  state.lastGapReceived =
    U;

  console.warn(
    `REAL DEPTH GAP expected=${expected} U=${U} u=${u}`
  );

  state.initialized = false;
  state.waitingForBridge = true;
  state.snapshotPending = true;

  /*
     Start a new buffer with the event
     that exposed the gap.
  */

  state.pendingDepthEvents = [
    event
  ];

  state.resyncs++;

  requestSnapshot(true);
}

/* ============================================================
   DEPTH WS
   ============================================================ */

function connectDepthWs() {
  if (
    state.depthWs &&
    (
      state.depthWs.readyState ===
        WebSocket.OPEN ||
      state.depthWs.readyState ===
        WebSocket.CONNECTING
    )
  ) {
    return;
  }

  const ws =
    new WebSocket(DEPTH_WS);

  state.depthWs = ws;

  ws.on("open", () => {
    state.depthConnected = true;

    state.initialized = false;
    state.waitingForBridge = true;
    state.snapshotPending = true;

    console.log(
      "Depth websocket connected"
    );

    requestSnapshot(true);
  });

  ws.on("message", raw => {
    try {
      const event =
        JSON.parse(
          raw.toString()
        );

      if (
        !event ||
        event.e !== "depthUpdate"
      ) {
        return;
      }

      /*
         Before synchronization:
         buffer every event.
      */

      if (!state.initialized) {
        state.pendingDepthEvents.push(
          event
        );

        if (
          state.pendingDepthEvents.length >
          SETTINGS.maxPendingDepthEvents
        ) {
          state.pendingDepthEvents =
            state.pendingDepthEvents.slice(
              -SETTINGS.maxPendingDepthEvents
            );
        }

        if (
          !state.snapshotInFlight
        ) {
          requestSnapshot();
        }

        return;
      }

      processLiveDepthEvent(event);
    } catch (error) {
      state.lastError =
        `Depth parse: ${error.message}`;
    }
  });

  ws.on("error", error => {
    state.lastError =
      `Depth WS: ${error.message}`;

    console.error(
      "Depth WS:",
      error.message
    );
  });

  ws.on("close", () => {
    state.depthConnected = false;
    state.initialized = false;
    state.waitingForBridge = true;
    state.snapshotPending = true;
    state.snapshotInFlight = false;

    console.log(
      "Depth websocket closed"
    );

    setTimeout(
      connectDepthWs,
      SETTINGS.depthReconnectMs
    );
  });
}

/* ============================================================
   SNAPSHOT WS
   ============================================================ */

function connectSnapshotWs() {
  if (
    state.snapshotWs &&
    (
      state.snapshotWs.readyState ===
        WebSocket.OPEN ||
      state.snapshotWs.readyState ===
        WebSocket.CONNECTING
    )
  ) {
    return;
  }

  const ws =
    new WebSocket(
      FUTURES_API_WS
    );

  state.snapshotWs = ws;

  ws.on("open", () => {
    state.snapshotConnected = true;

    console.log(
      "Snapshot websocket connected"
    );

    if (
      state.depthConnected &&
      !state.initialized
    ) {
      requestSnapshot(true);
    }
  });

  ws.on("message", raw => {
    try {
      const message =
        JSON.parse(
          raw.toString()
        );

      if (message.error) {
        if (
          message.error.code === -1003
        ) {
          state.snapshot429s++;
        }

        state.lastError =
          `Snapshot API: ${
            message.error.msg ||
            JSON.stringify(
              message.error
            )
          }`;

        state.snapshotInFlight = false;

        return;
      }

      if (message.result) {
        processSnapshot(
          message.result
        );
      }
    } catch (error) {
      state.lastError =
        `Snapshot parse: ${error.message}`;
    }
  });

  ws.on("error", error => {
    state.lastError =
      `Snapshot WS: ${error.message}`;

    console.error(
      "Snapshot WS:",
      error.message
    );
  });

  ws.on("close", () => {
    state.snapshotConnected = false;
    state.snapshotInFlight = false;

    console.log(
      "Snapshot websocket closed"
    );

    setTimeout(
      connectSnapshotWs,
      SETTINGS.snapshotReconnectMs
    );
  });
}

/* ============================================================
   HEALTH
   ============================================================ */

function healthResponse() {
  updateCurrentPrice();

  const structure =
    state.structure;

  return {
    ok: true,

    service:
      "BTCUSDT Liquidity Relay",

    version: "11.0",

    symbol: SYMBOL,

    status:
      state.initialized
        ? "live"
        : "syncing",

    websocketConnected:
      state.depthConnected,

    depthConnected:
      state.depthConnected,

    snapshotConnected:
      state.snapshotConnected,

    klineConnected:
      state.klineConnected,

    initialized:
      state.initialized,

    waitingForBridge:
      state.waitingForBridge,

    snapshotPending:
      state.snapshotPending,

    snapshotInFlight:
      state.snapshotInFlight,

    currentPrice:
      state.currentPrice,

    priceSource:
      state.priceSource,

    bidLevels:
      state.bids.size,

    askLevels:
      state.asks.size,

    lastUpdateId:
      state.lastUpdateId,

    pendingEvents:
      state.pendingDepthEvents.length,

    snapshotRequests:
      state.snapshotRequests,

    snapshot429s:
      state.snapshot429s,

    bridgeAttempts:
      state.bridgeAttempts,

    bridgeFound:
      state.bridgeFound,

    resyncs:
      state.resyncs,

    sequenceGaps:
      state.sequenceGaps,

    lastSnapshotId:
      state.lastSnapshotId,

    lastBridgeU:
      state.lastBridgeU,

    lastGapExpected:
      state.lastGapExpected,

    lastGapReceived:
      state.lastGapReceived,

    structure: {
      status:
        structure.status,

      activeLevels:
        structure.levels.length,

      activeMajorBSL:
        getMajorLevels(
          "BSL"
        ).length,

      activeMajorSSL:
        getMajorLevels(
          "SSL"
        ).length,

      lastRebuildAt:
        structure.lastRebuildAt,

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

    startedAt:
      state.startedAt,

    updatedAt:
      state.updatedAt,

    lastError:
      state.lastError
  };
}

/* ============================================================
   HEALTH ROUTES
   ============================================================ */

/*
   These routes intentionally do NOT depend on
   Binance, the order book, structure, or startup.

   Render can therefore always determine whether
   the Node process itself is alive.
*/

app.get("/health", (req, res) => {
  res.status(200).json(
    healthResponse()
  );
});

app.get("/healthz", (req, res) => {
  res.status(200).json({
    ok: true,
    status: "alive",
    service:
      "BTCUSDT Liquidity Relay",
    version: "11.0",
    time:
      new Date().toISOString()
  });
});

app.get("/ping", (req, res) => {
  res.status(200).send("pong");
});

app.head("/health", (req, res) => {
  res.status(200).end();
});

/* ============================================================
   BOOK
   ============================================================ */

app.get("/book", (req, res) => {
  updateCurrentPrice();

  const bids =
    getBookRows("bid")
      .sort(
        (a, b) =>
          b.price - a.price
      )
      .slice(0, 100);

  const asks =
    getBookRows("ask")
      .sort(
        (a, b) =>
          a.price - b.price
      )
      .slice(0, 100);

  res.status(200).json({
    ok: true,
    symbol: SYMBOL,
    currentPrice:
      state.currentPrice,
    initialized:
      state.initialized,
    lastUpdateId:
      state.lastUpdateId,
    bids,
    asks,
    updatedAt:
      new Date().toISOString()
  });
});

/* ============================================================
   LIQUIDITY
   ============================================================ */

app.get("/liquidity", (req, res) => {
  updateCurrentPrice();

  res.status(200).json({
    ok: true,
    symbol: SYMBOL,
    currentPrice:
      state.currentPrice,

    orderBook:
      buildOrderBookIntelligence(),

    persistentLiquidity: {
      bids:
        state.persistence.bids,
      asks:
        state.persistence.asks
    },

    updatedAt:
      new Date().toISOString()
  });
});

/* ============================================================
   STRUCTURE
   ============================================================ */

app.get("/structure", (req, res) => {
  updateCurrentPrice();

  res.status(200).json({
    ok: true,

    symbol: SYMBOL,

    currentPrice:
      state.currentPrice,

    majorLevels:
      state.structure.majorLevels,

    nearLevels:
      state.structure.nearLevels,

    allLevels:
      state.structure.levels,

    structureStatus: {
      activeLevels:
        state.structure.levels.length,

      activeMajorBSL:
        getMajorLevels(
          "BSL"
        ).length,

      activeMajorSSL:
        getMajorLevels(
          "SSL"
        ).length,

      lastRebuildAt:
        state.structure.lastRebuildAt,

      lastGoodAt:
        state.structure.lastGoodAt,

      lastRebuildAccepted:
        state.structure.lastRebuildAccepted,

      lastRebuildReason:
        state.structure.lastRebuildReason,

      rebuildCount:
        state.structure.rebuildCount,

      rejectedRebuilds:
        state.structure.rejectedRebuilds,

      lastCandidateLevels:
        state.structure.lastCandidateLevels,

      lastCandidateMajor:
        state.structure.lastCandidateMajor,

      retainedPrevious:
        state.structure.retainedPrevious,

      status:
        state.structure.status
    },

    updatedAt:
      new Date().toISOString()
  });
});

/* ============================================================
   DISTANCE
   ============================================================ */

function addDistance(level) {
  return {
    ...level,

    distancePct:
      round(
        distancePct(
          level.price,
          state.currentPrice
        ),
        3
      )
  };
}

/* ============================================================
   INTELLIGENCE
   ============================================================ */

app.get(
  "/intelligence",
  (req, res) => {
    updateCurrentPrice();

    const price =
      state.currentPrice;

    const majorBSL =
      getMajorLevels("BSL");

    const majorSSL =
      getMajorLevels("SSL");

    const nearBSL =
      getNearLevels("BSL");

    const nearSSL =
      getNearLevels("SSL");

    const nearestBSL =
      majorBSL[0] || null;

    const nearestSSL =
      majorSSL[0] || null;

    let marketState =
      "NO MAJOR STRUCTURAL LIQUIDITY";

    if (
      nearestBSL &&
      nearestSSL &&
      price <
        nearestBSL.price &&
      price >
        nearestSSL.price
    ) {
      marketState =
        "BETWEEN MAJOR LIQUIDITY";
    }

    res.status(200).json({
      ok: true,

      symbol: SYMBOL,

      currentPrice: price,

      priceSource:
        state.priceSource,

      intelligence: {
        state:
          marketState,

        structureStatus: {
          activeLevels:
            state.structure.levels.length,

          activeMajorBSL:
            majorBSL.length,

          activeMajorSSL:
            majorSSL.length,

          lastRebuildAt:
            state.structure.lastRebuildAt,

          lastGoodAt:
            state.structure.lastGoodAt,

          lastRebuildAccepted:
            state.structure.lastRebuildAccepted,

          lastRebuildReason:
            state.structure.lastRebuildReason,

          rebuildCount:
            state.structure.rebuildCount,

          rejectedRebuilds:
            state.structure.rejectedRebuilds,

          lastCandidateLevels:
            state.structure.lastCandidateLevels,

          lastCandidateMajor:
            state.structure.lastCandidateMajor,

          retainedPrevious:
            state.structure.retainedPrevious,

          status:
            state.structure.status
        },

        nearestBSL:
          nearestBSL
            ? addDistance(
                nearestBSL
              )
            : null,

        nearestSSL:
          nearestSSL
            ? addDistance(
                nearestSSL
              )
            : null,

        /*
           BSL list is ABOVE price.
           SSL list is BELOW price.
        */

        nextBSL:
          majorBSL
            .slice(1, 6)
            .map(addDistance),

        nextSSL:
          majorSSL
            .slice(1, 6)
            .map(addDistance),

        nearBSL:
          nearBSL
            .slice(0, 8)
            .map(addDistance),

        nearSSL:
          nearSSL
            .slice(0, 8)
            .map(addDistance),

        orderBook:
          buildOrderBookIntelligence(),

        persistentLiquidity: {
          bids:
            state.persistence.bids,

          asks:
            state.persistence.asks
        },

        liquidityEvents:
          state.liquidityEvents.slice(
            0,
            50
          ),

        recentSweeps:
          state.recentSweeps.slice(
            0,
            20
          )
      },

      updatedAt:
        new Date().toISOString()
    });
  }
);

/* ============================================================
   EVENTS
   ============================================================ */

app.get("/events", (req, res) => {
  res.status(200).json({
    ok: true,

    symbol: SYMBOL,

    currentPrice:
      state.currentPrice,

    recentSweeps:
      state.recentSweeps,

    liquidityEvents:
      state.liquidityEvents,

    persistentLiquidity: {
      bids:
        state.persistence.bids,

      asks:
        state.persistence.asks
    },

    updatedAt:
      new Date().toISOString()
  });
});

/* ============================================================
   ROOT
   ============================================================ */

app.get("/", (req, res) => {
  res.status(200).json({
    ok: true,

    service:
      "BTCUSDT Major Liquidity Relay",

    version:
      "11.0",

    health:
      "/health",

    endpoints: [
      "/health",
      "/healthz",
      "/ping",
      "/book",
      "/liquidity",
      "/structure",
      "/intelligence",
      "/events"
    ]
  });
});

/* ============================================================
   ERROR HANDLER
   ============================================================ */

app.use(
  (err, req, res, next) => {
    state.lastError =
      `Express: ${
        err.message ||
        String(err)
      }`;

    console.error(
      "Express error:",
      err
    );

    res.status(500).json({
      ok: false,
      error:
        err.message ||
        "Internal server error"
    });
  }
);

/* ============================================================
   PROCESS ERROR PROTECTION
   ============================================================ */

process.on(
  "uncaughtException",
  error => {
    state.lastError =
      `uncaughtException: ${error.message}`;

    console.error(
      "UNCAUGHT EXCEPTION:",
      error
    );
  }
);

process.on(
  "unhandledRejection",
  reason => {
    state.lastError =
      `unhandledRejection: ${
        reason?.message ||
        String(reason)
      }`;

    console.error(
      "UNHANDLED REJECTION:",
      reason
    );
  }
);

/* ============================================================
   PERSISTENCE LOOP
   ============================================================ */

setInterval(() => {
  try {
    if (state.initialized) {
      updatePersistence();
    }
  } catch (error) {
    state.lastError =
      `Persistence: ${error.message}`;

    console.error(
      "Persistence:",
      error.message
    );
  }
}, SETTINGS.persistencePollMs);

/* ============================================================
   MAINTENANCE
   ============================================================ */

setInterval(() => {
  try {
    updateCurrentPrice();

    if (
      state.structure.levels.length
    ) {
      for (
        const level of
        state.structure.levels
      ) {
        getRestingLiquidity(level);
      }

      for (
        const level of
        state.structure.majorLevels
      ) {
        getRestingLiquidity(level);
      }
    }

    /*
       Remove old sweeps.
    */

    const cutoff =
      now() -
      SETTINGS.sweepLookbackMs;

    state.recentSweeps =
      state.recentSweeps.filter(
        x =>
          x.detectedAt &&
          new Date(
            x.detectedAt
          ).getTime() >= cutoff
      );
  } catch (error) {
    state.lastError =
      `Maintenance: ${error.message}`;
  }
}, 5000);

/* ============================================================
   STARTUP
   ============================================================ */

async function startup() {
  console.log(
    "=============================================="
  );

  console.log(
    "BTCUSDT LIQUIDITY RELAY V11"
  );

  console.log(
    "=============================================="
  );

  connectDepthWs();
  connectSnapshotWs();
  connectKlineWs();

  await loadAllHistoricalCandles();

  /*
     Give live book a few seconds to establish
     current price before rebuilding directional
     structure.
  */

  setTimeout(() => {
    try {
      updateCurrentPrice();
      rebuildStructure();

      console.log(
        "Initial structure rebuild complete"
      );
    } catch (error) {
      state.lastError =
        `Initial rebuild: ${error.message}`;
    }
  }, 5000);
}

/* ============================================================
   SERVER
   ============================================================ */

/*
   Explicit 0.0.0.0 binding is important for Render.
*/

const server =
  app.listen(
    PORT,
    "0.0.0.0",
    () => {
      console.log(
        `HTTP server listening on 0.0.0.0:${PORT}`
      );

      console.log(
        `Health: /health`
      );

      console.log(
        `Healthz: /healthz`
      );

      console.log(
        `Ping: /ping`
      );

      startup()
        .catch(error => {
          state.lastError =
            `Startup: ${error.message}`;

          console.error(
            "Startup error:",
            error
          );
        });
    }
  );

server.on(
  "error",
  error => {
    state.lastError =
      `HTTP server: ${error.message}`;

    console.error(
      "HTTP server error:",
      error
    );
  }
);
