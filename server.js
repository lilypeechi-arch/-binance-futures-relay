const express = require('express');
const WebSocket = require('ws');
const axios = require('axios');

const app = express();
const PORT = process.env.PORT || 10000;
const SYMBOL = 'BTCUSDT';
const S = SYMBOL.toLowerCase();

const SPOT_KLINES = 'https://data-api.binance.vision/api/v3/klines';
const DEPTH_WS = `wss://fstream.binance.com/ws/${S}@depth@100ms`;
const KLINE_WS = `wss://fstream.binance.com/stream?streams=${S}@kline_15m/${S}@kline_1h/${S}@kline_4h/${S}@kline_1d`;
const SNAPSHOT_WS = 'wss://ws-fapi.binance.com/ws-fapi/v1';

/* =========================================================
   TIMEFRAMES
========================================================= */

const TF = {
  '15m': {
    interval: '15m',
    limit: 300,
    left: 3,
    right: 3,
    weight: 1,
    equal: 0.0015
  },

  '1h': {
    interval: '1h',
    limit: 300,
    left: 4,
    right: 4,
    weight: 2,
    equal: 0.0015
  },

  '4h': {
    interval: '4h',
    limit: 250,
    left: 5,
    right: 5,
    weight: 4,
    equal: 0.0020
  },

  '1d': {
    interval: '1d',
    limit: 180,
    left: 5,
    right: 5,
    weight: 6,
    equal: 0.0025
  }
};

/* =========================================================
   CANDLES
========================================================= */

const candles = {
  '15m': [],
  '1h': [],
  '4h': [],
  '1d': []
};

const candleSource = {
  '15m': 'none',
  '1h': 'none',
  '4h': 'none',
  '1d': 'none'
};

const candleStats = {};

for (const tf of Object.keys(TF)) {
  candleStats[tf] = {
    requests: 0,
    success: 0,
    errors: 0
  };
}

/* =========================================================
   ORDER BOOK
========================================================= */

const bids = new Map();
const asks = new Map();

let currentPrice = null;

let depthLastUpdateId = 0;
let bookInitialized = false;

let depthConnected = false;
let klineConnected = false;
let snapshotConnected = false;

let depthWs = null;
let klineWs = null;
let snapshotWs = null;

let pending = [];

let gapRecovery = false;
let gapDetected = false;
let gapCount = 0;

let lastGapLocal = null;
let lastGapIncoming = null;

let snapshotPending = false;
let snapshotRequests = 0;
let snapshot429s = 0;

let lastSnapshotRequest = 0;
let lastSnapshotResponse = null;

let snapshotLastStatus = null;
let snapshotLastError = null;

const SNAPSHOT_COOLDOWN = 65000;

/* =========================================================
   STRUCTURE
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
   MARKET STATE
========================================================= */

let marketState = {
  state: 'NEUTRAL',
  direction: 'NONE',
  level: null,
  side: null,
  swept: false,
  rejected: false,
  structureShift: false,
  updatedAt: null
};

/* =========================================================
   HELPERS
========================================================= */

function iso() {
  return new Date().toISOString();
}

function round(value) {
  return Number(Number(value).toFixed(2));
}

function pct(a, b) {
  if (!a || !b) return 999;
  return Math.abs(a - b) / b;
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function priceZone(price) {
  const width = price * 0.00035;

  return {
    low: price - width,
    high: price + width
  };
}

/* =========================================================
   HISTORICAL SPOT CANDLES
========================================================= */

async function loadHistory(tf) {
  const cfg = TF[tf];

  candleStats[tf].requests++;

  try {
    const response = await axios.get(
      SPOT_KLINES,
      {
        params: {
          symbol: SYMBOL,
          interval: cfg.interval,
          limit: cfg.limit
        },

        timeout: 15000,

        headers: {
          'User-Agent': 'Mozilla/5.0'
        }
      }
    );

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
        source: 'spot'
      });
    }

    if (!result.length) {
      throw new Error('No candles returned');
    }

    candles[tf] = result;

    candleSource[tf] = 'binance_spot';

    candleStats[tf].success++;

    console.log(
      `[HISTORY] ${tf}: ${result.length}`
    );

    return true;

  } catch (error) {

    candleStats[tf].errors++;

    console.log(
      `[HISTORY ERROR] ${tf}: ${error.response?.status || ''} ${error.message}`
    );

    return false;
  }
}

/* =========================================================
   LOAD ALL HISTORY
========================================================= */

async function loadAllHistory() {

  for (const tf of Object.keys(TF)) {

    await loadHistory(tf);

    await sleep(250);
  }

  rebuildStructure();
}

/* =========================================================
   PIVOT HIGH
========================================================= */

function isPivotHigh(
  list,
  index,
  left,
  right
) {

  if (
    index < left ||
    index + right >= list.length
  ) {
    return false;
  }

  const value =
    list[index].high;

  for (
    let i = index - left;
    i <= index + right;
    i++
  ) {

    if (i === index) {
      continue;
    }

    if (
      list[i].high >= value
    ) {
      return false;
    }
  }

  return true;
}

/* =========================================================
   PIVOT LOW
========================================================= */

function isPivotLow(
  list,
  index,
  left,
  right
) {

  if (
    index < left ||
    index + right >= list.length
  ) {
    return false;
  }

  const value =
    list[index].low;

  for (
    let i = index - left;
    i <= index + right;
    i++
  ) {

    if (i === index) {
      continue;
    }

    if (
      list[i].low <= value
    ) {
      return false;
    }
  }

  return true;
}

/* =========================================================
   FIND SWINGS
========================================================= */

function swings(tf) {

  const cfg = TF[tf];
  const list = candles[tf];

  const highs = [];
  const lows = [];

  for (
    let i = cfg.left;
    i < list.length - cfg.right;
    i++
  ) {

    if (
      isPivotHigh(
        list,
        i,
        cfg.left,
        cfg.right
      )
    ) {

      highs.push({
        timeframe: tf,
        price: list[i].high,
        time: list[i].openTime
      });
    }

    if (
      isPivotLow(
        list,
        i,
        cfg.left,
        cfg.right
      )
    ) {

      lows.push({
        timeframe: tf,
        price: list[i].low,
        time: list[i].openTime
      });
    }
  }

  return {
    highs,
    lows
  };
}

/* =========================================================
   EQUAL HIGH / LOW GROUPS
========================================================= */

function buildEqualGroups(
  levels,
  tolerance
) {

  const groups = [];

  for (const level of levels) {

    let group =
      groups.find(
        g =>
          pct(
            level.price,
            g.price
          ) <= tolerance
      );

    if (!group) {

      group = {
        price: level.price,
        count: 0,
        timeframes: new Set(),
        levels: []
      };

      groups.push(group);
    }

    group.levels.push(level);

    group.price =
      group.levels.reduce(
        (sum, item) =>
          sum + item.price,
        0
      ) /
      group.levels.length;

    group.count =
      group.levels.length;

    group.timeframes.add(
      level.timeframe
    );
  }

  return groups
    .filter(
      g =>
        g.count >= 2
    )
    .map(
      g => ({
        price: g.price,
        count: g.count,
        timeframes:
          [...g.timeframes]
      })
    );
}

/* =========================================================
   MERGE STRUCTURAL LEVELS
========================================================= */

function mergeStructural(
  levels
) {

  const groups = [];

  const sorted =
    [...levels].sort(
      (a, b) =>
        a.price - b.price
    );

  for (const level of sorted) {

    let group =
      groups.find(
        g =>
          Math.abs(
            g.price -
            level.price
          ) <= 35
      );

    if (!group) {

      group = {
        prices: [],
        timeframes: new Set(),
        levels: [],
        price: level.price
      };

      groups.push(group);
    }

    group.prices.push(
      level.price
    );

    group.levels.push(
      level
    );

    group.timeframes.add(
      level.timeframe
    );

    group.price =
      group.prices.reduce(
        (a, b) =>
          a + b,
        0
      ) /
      group.prices.length;
  }

  return groups.map(
    g => ({
      price: g.price,

      priceLow:
        Math.min(
          ...g.prices
        ),

      priceHigh:
        Math.max(
          ...g.prices
        ),

      touches:
        g.prices.length,

      timeframes:
        [...g.timeframes],

      sourceLevels:
        g.levels
    })
  );
}

/* =========================================================
   CURRENT RESTING LIQUIDITY
========================================================= */

function restingNear(
  price,
  side
) {

  const book =
    side === 'BSL'
      ? asks
      : bids;

  const levels = [];

  let totalUsd = 0;

  for (
    const [
      priceText,
      quantity
    ] of book
  ) {

    const levelPrice =
      Number(priceText);

    const qty =
      Number(quantity);

    const usd =
      levelPrice * qty;

    if (
      !Number.isFinite(
        levelPrice
      ) ||
      !Number.isFinite(
        qty
      ) ||
      usd < 50000
    ) {
      continue;
    }

    if (
      Math.abs(
        levelPrice -
        price
      ) > 35
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
   DISTINCT REACTION COUNT
========================================================= */

function distinctReactions(
  price,
  tf,
  side
) {

  const list =
    candles[tf];

  if (
    list.length < 10
  ) {
    return 0;
  }

  const zone =
    price * 0.0012;

  const minMove =
    price * 0.003;

  let reactions = 0;

  let lastReaction =
    -999;

  for (
    let i = 0;
    i < list.length - 5;
    i++
  ) {

    const candle =
      list[i];

    const touched =
      side === 'BSL'
        ? candle.high >=
            price - zone &&
          candle.low <=
            price + zone
        : candle.low <=
            price + zone &&
          candle.high >=
            price - zone;

    if (
      !touched
    ) {
      continue;
    }

    if (
      i - lastReaction < 5
    ) {
      continue;
    }

    let futureHigh =
      candle.high;

    let futureLow =
      candle.low;

    for (
      let j = i + 1;
      j <=
        Math.min(
          i + 5,
          list.length - 1
        );
      j++
    ) {

      futureHigh =
        Math.max(
          futureHigh,
          list[j].high
        );

      futureLow =
        Math.min(
          futureLow,
          list[j].low
        );
    }

    const rejection =
      side === 'BSL'
        ? price -
            futureLow >=
          minMove
        : futureHigh -
            price >=
          minMove;

    if (
      rejection
    ) {

      reactions++;

      lastReaction =
        i;
    }
  }

  return reactions;
}

/* =========================================================
   STRUCTURAL SCORE
========================================================= */

function scoreLevel(
  level,
  side,
  equalGroups
) {

  let score = 0;

  const tfSet =
    new Set(
      level.timeframes
    );

  /*
     HIGHER TIMEFRAME WEIGHT
  */

  if (
    tfSet.has('1d')
  ) {
    score += 45;
  }

  if (
    tfSet.has('4h')
  ) {
    score += 35;
  }

  if (
    tfSet.has('1h')
  ) {
    score += 15;
  }

  if (
    tfSet.has('15m')
  ) {
    score += 5;
  }

  /*
     MULTI-TIMEFRAME AGREEMENT
  */

  if (
    tfSet.size >= 2
  ) {
    score += 12;
  }

  if (
    tfSet.size >= 3
  ) {
    score += 15;
  }

  /*
     EQUAL HIGHS / LOWS
  */

  let equalCount = 0;

  for (
    const equal of equalGroups
  ) {

    if (
      pct(
        equal.price,
        level.price
      ) <= 0.002
    ) {

      equalCount +=
        equal.count;
    }
  }

  score +=
    Math.min(
      equalCount * 7,
      28
    );

  /*
     DISTINCT REACTIONS
  */

  let reactions = 0;

  for (
    const tf of level.timeframes
  ) {

    reactions +=
      distinctReactions(
        level.price,
        tf,
        side
      );
  }

  score +=
    Math.min(
      reactions * 4,
      24
    );

  /*
     RESTING LIQUIDITY CONFIRMATION
  */

  const resting =
    restingNear(
      level.price,
      side
    );

  if (
    resting.totalUsd >=
    1000000
  ) {

    score += 15;

  } else if (
    resting.totalUsd >=
    500000
  ) {

    score += 10;

  } else if (
    resting.totalUsd >=
    250000
  ) {

    score += 6;
  }

  return {
    ...level,

    score:
      Math.round(
        score
      ),

    reactions,

    equalCount,

    restingUsd:
      resting.totalUsd,

    restingLevels:
      resting.levels
  };
}

/* =========================================================
   FORMAT LEVEL
========================================================= */

function formatLevel(
  level
) {

  const zone =
    priceZone(
      level.price
    );

  return {

    side:
      level.side,

    price:
      round(
        level.price
      ),

    priceLow:
      round(
        Math.min(
          level.priceLow ??
            zone.low,
          zone.low
        )
      ),

    priceHigh:
      round(
        Math.max(
          level.priceHigh ??
            zone.high,
          zone.high
        )
      ),

    score:
      level.score,

    strength:
      level.score >= 100
        ? 'MAJOR'
        : level.score >= 70
          ? 'STRONG'
          : 'SECONDARY',

    timeframes:
      level.timeframes,

    equalCount:
      level.equalCount,

    reactions:
      level.reactions,

    restingUsd:
      round(
        level.restingUsd
      ),

    restingLevels:
      level.restingLevels,

    distancePct:
      currentPrice == null
        ? null
        : Number(
            (
              (
                level.price -
                currentPrice
              ) /
              currentPrice *
              100
            ).toFixed(3)
          ),

    source:
      'spot_structure+futures_liquidity'
  };
}

/* =========================================================
   REBUILD STRUCTURE
========================================================= */

function rebuildStructure() {

  const highs = [];
  const lows = [];

  const allEqualHighs = [];
  const allEqualLows = [];

  for (
    const tf of Object.keys(TF)
  ) {

    const result =
      swings(tf);

    highs.push(
      ...result.highs
    );

    lows.push(
      ...result.lows
    );

    allEqualHighs.push(
      ...buildEqualGroups(
        result.highs,
        TF[tf].equal
      )
    );

    allEqualLows.push(
      ...buildEqualGroups(
        result.lows,
        TF[tf].equal
      )
    );
  }

  structure.swingHighs =
    highs;

  structure.swingLows =
    lows;

  structure.equalHighs =
    allEqualHighs;

  structure.equalLows =
    allEqualLows;

  const bsl =
    mergeStructural(
      highs
    )
      .map(
        level => ({
          ...scoreLevel(
            level,
            'BSL',
            allEqualHighs
          ),
          side: 'BSL'
        })
      )
      .filter(
        level =>
          level.timeframes.includes(
            '4h'
          ) ||
          level.timeframes.includes(
            '1d'
          ) ||
          (
            level.equalCount >= 4 &&
            level.score >= 70
          )
      )
      .filter(
        level =>
          level.score >= 65
      )
      .sort(
        (a, b) =>
          b.score - a.score
      );

  const ssl =
    mergeStructural(
      lows
    )
      .map(
        level => ({
          ...scoreLevel(
            level,
            'SSL',
            allEqualLows
          ),
          side: 'SSL'
        })
      )
      .filter(
        level =>
          level.timeframes.includes(
            '4h'
          ) ||
          level.timeframes.includes(
            '1d'
          ) ||
          (
            level.equalCount >= 4 &&
            level.score >= 70
          )
      )
      .filter(
        level =>
          level.score >= 65
      )
      .sort(
        (a, b) =>
          b.score - a.score
      );

  structure.majorBSL =
    bsl.slice(
      0,
      8
    );

  structure.majorSSL =
    ssl.slice(
      0,
      8
    );

  structure.majorResistance =
    bsl
      .filter(
        level =>
          currentPrice == null ||
          level.price >
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

  structure.majorSupport =
    ssl
      .filter(
        level =>
          currentPrice == null ||
          level.price <
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

  structure.updatedAt =
    iso();

  updateMarketState();
}

/* =========================================================
   LIQUIDITY SWEEP DETECTION
========================================================= */

function detectSweep(
  level
) {

  if (
    currentPrice == null ||
    !candles['15m'].length
  ) {
    return false;
  }

  const list =
    candles['15m'];

  const recent =
    list.slice(
      -6
    );

  const zone =
    Math.max(
      level.price *
        0.0012,
      20
    );

  if (
    level.side === 'BSL'
  ) {

    return recent.some(
      candle =>
        candle.high >=
          level.price -
            zone &&
        candle.close <
          level.price -
            zone *
              0.15
    );
  }

  return recent.some(
    candle =>
      candle.low <=
        level.price +
          zone &&
      candle.close >
        level.price +
          zone *
            0.15
  );
}

/* =========================================================
   REJECTION DETECTION
========================================================= */

function detectRejection(
  level
) {

  if (
    !candles['15m'].length
  ) {
    return false;
  }

  const candle =
    candles['15m'][
      candles['15m'].length - 1
    ];

  const zone =
    Math.max(
      level.price *
        0.001,
      15
    );

  if (
    level.side === 'BSL'
  ) {

    return (
      candle.high >=
        level.price -
          zone &&
      candle.close <
        candle.open &&
      candle.close <
        level.price
    );
  }

  return (
    candle.low <=
      level.price +
        zone &&
    candle.close >
      candle.open &&
    candle.close >
      level.price
  );
}

/* =========================================================
   STRUCTURE SHIFT
========================================================= */

function detectStructureShift(
  side
) {

  const list =
    candles['15m'];

  if (
    list.length < 30
  ) {
    return false;
  }

  const recent =
    list.slice(
      -12
    );

  const prior =
    list.slice(
      -24,
      -12
    );

  const recentHigh =
    Math.max(
      ...recent.map(
        c => c.high
      )
    );

  const recentLow =
    Math.min(
      ...recent.map(
        c => c.low
      )
    );

  const priorHigh =
    Math.max(
      ...prior.map(
        c => c.high
      )
    );

  const priorLow =
    Math.min(
      ...prior.map(
        c => c.low
      )
    );

  if (
    side === 'BSL'
  ) {

    return (
      recentLow <
      priorLow
    );
  }

  return (
    recentHigh >
    priorHigh
  );
}

/* =========================================================
   MARKET STATE
========================================================= */

function updateMarketState() {

  if (
    currentPrice == null
  ) {
    return;
  }

  const above =
    structure.majorBSL
      .filter(
        level =>
          level.price >=
          currentPrice
      )
      .sort(
        (a, b) =>
          a.price - b.price
      );

  const below =
    structure.majorSSL
      .filter(
        level =>
          level.price <=
          currentPrice
      )
      .sort(
        (a, b) =>
          b.price - a.price
      );

  const candidates = [];

  if (
    above[0]
  ) {

    candidates.push({
      level:
        above[0],

      distance:
        pct(
          above[0].price,
          currentPrice
        )
    });
  }

  if (
    below[0]
  ) {

    candidates.push({
      level:
        below[0],

      distance:
        pct(
          below[0].price,
          currentPrice
        )
    });
  }

  if (
    !candidates.length
  ) {
    return;
  }

  candidates.sort(
    (a, b) =>
      a.distance -
      b.distance
  );

  const target =
    candidates[0].level;

  const swept =
    detectSweep(
      target
    );

  const rejected =
    swept &&
    detectRejection(
      target
    );

  const shifted =
    rejected &&
    detectStructureShift(
      target.side
    );

  let state =
    'APPROACHING_LIQUIDITY';

  if (
    swept
  ) {
    state =
      'LIQUIDITY_SWEPT';
  }

  if (
    rejected
  ) {
    state =
      'REJECTION';
  }

  if (
    shifted
  ) {
    state =
      'STRUCTURE_SHIFT';
  }

  marketState = {

    state,

    direction:
      target.side === 'BSL'
        ? 'BEARISH_REVERSAL_WATCH'
        : 'BULLISH_REVERSAL_WATCH',

    level:
      formatLevel(
        target
      ),

    side:
      target.side,

    swept,

    rejected,

    structureShift:
      shifted,

    updatedAt:
      iso()
  };
}

/* =========================================================
   DEPTH UPDATE
========================================================= */

function applyDepth(
  data
) {

  const U =
    Number(data.U);

  const u =
    Number(data.u);

  if (
    !Number.isFinite(U) ||
    !Number.isFinite(u)
  ) {
    return;
  }

  if (
    !bookInitialized
  ) {

    pending.push(
      data
    );

    if (
      pending.length >
      20000
    ) {

      pending =
        pending.slice(
          -20000
        );
    }

    return;
  }

  const expected =
    depthLastUpdateId +
    1;

  if (
    U > expected
  ) {

    lastGapLocal =
      depthLastUpdateId;

    lastGapIncoming =
      U;

    gapDetected =
      true;

    if (
      !gapRecovery
    ) {

      gapRecovery =
        true;

      gapCount++;

      console.log(
        '[DEPTH] GAP',
        expected,
        U
      );
    }

    return;
  }

  if (
    u <=
    depthLastUpdateId
  ) {
    return;
  }

  for (
    const item of
      data.b || []
  ) {

    if (
      Number(item[1]) ===
      0
    ) {

      bids.delete(
        item[0]
      );

    } else {

      bids.set(
        item[0],
        Number(item[1])
      );
    }
  }

  for (
    const item of
      data.a || []
  ) {

    if (
      Number(item[1]) ===
      0
    ) {

      asks.delete(
        item[0]
      );

    } else {

      asks.set(
        item[0],
        Number(item[1])
      );
    }
  }

  depthLastUpdateId =
    u;

  updatePrice();
}

/* =========================================================
   PRICE
========================================================= */

function updatePrice() {

  let bestBid =
    null;

  let bestAsk =
    null;

  for (
    const priceText of
      bids.keys()
  ) {

    const price =
      Number(priceText);

    if (
      bestBid === null ||
      price > bestBid
    ) {

      bestBid =
        price;
    }
  }

  for (
    const priceText of
      asks.keys()
  ) {

    const price =
      Number(priceText);

    if (
      bestAsk === null ||
      price < bestAsk
    ) {

      bestAsk =
        price;
    }
  }

  if (
    bestBid !== null &&
    bestAsk !== null
  ) {

    currentPrice =
      (
        bestBid +
        bestAsk
      ) / 2;
  }
}

/* =========================================================
   SNAPSHOT
========================================================= */

function requestSnapshot() {

  const now =
    Date.now();

  if (
    snapshotPending
  ) {
    return;
  }

  if (
    now -
      lastSnapshotRequest <
    SNAPSHOT_COOLDOWN
  ) {
    return;
  }

  lastSnapshotRequest =
    now;

  snapshotPending =
    true;

  snapshotRequests++;

  try {

    if (
      snapshotWs
    ) {

      try {
        snapshotWs.close();
      } catch (_) {}
    }

    snapshotWs =
      new WebSocket(
        SNAPSHOT_WS
      );

    snapshotWs.on(
      'open',
      () => {

        snapshotConnected =
          true;

        snapshotWs.send(
          JSON.stringify({
            id:
              String(
                Date.now()
              ),

            method:
              'depth',

            params: {
              symbol:
                SYMBOL,

              limit:
                1000
            }
          })
        );
      }
    );

    snapshotWs.on(
      'message',
      raw => {

        try {

          const message =
            JSON.parse(
              raw.toString()
            );

          if (
            message.status &&
            message.status !==
              200
          ) {

            snapshotLastStatus =
              message.status;

            snapshotLastError =
              JSON.stringify(
                message.error ||
                message
              );

            if (
              message.status ===
              429
            ) {

              snapshot429s++;
            }

            snapshotPending =
              false;

            return;
          }

          if (
            !message.result ||
            !message.result
              .lastUpdateId
          ) {
            return;
          }

          const result =
            message.result;

          bids.clear();
          asks.clear();

          for (
            const item of
              result.bids || []
          ) {

            if (
              Number(item[1]) >
              0
            ) {

              bids.set(
                item[0],
                Number(item[1])
              );
            }
          }

          for (
            const item of
              result.asks || []
          ) {

            if (
              Number(item[1]) >
              0
            ) {

              asks.set(
                item[0],
                Number(item[1])
              );
            }
          }

          depthLastUpdateId =
            Number(
              result.lastUpdateId
            );

          bookInitialized =
            true;

          const buffered =
            [...pending].sort(
              (a, b) =>
                Number(a.U) -
                Number(b.U)
            );

          pending = [];

          const target =
            depthLastUpdateId +
            1;

          const bridge =
            buffered.findIndex(
              item =>
                Number(item.U) <=
                  target &&
                Number(item.u) >=
                  target
            );

          if (
            bridge >= 0
          ) {

            for (
              let i = bridge;
              i < buffered.length;
              i++
            ) {

              applyDepth(
                buffered[i]
              );
            }

            gapRecovery =
              false;

            gapDetected =
              false;

          } else if (
            buffered.length
          ) {

            gapRecovery =
              true;

            gapDetected =
              true;

          } else {

            gapRecovery =
              false;

            gapDetected =
              false;
          }

          updatePrice();

          snapshotLastStatus =
            200;

          snapshotLastError =
            null;

          lastSnapshotResponse =
            iso();

          snapshotPending =
            false;

          rebuildStructure();

          console.log(
            `[SNAPSHOT] Installed ${depthLastUpdateId} bids=${bids.size} asks=${asks.size}`
          );

        } catch (error) {

          snapshotLastError =
            error.message;

          snapshotPending =
            false;
        }
      }
    );

    snapshotWs.on(
      'error',
      error => {

        snapshotLastError =
          error.message;

        snapshotPending =
          false;
      }
    );

    snapshotWs.on(
      'close',
      () => {

        snapshotConnected =
          false;
      }
    );

  } catch (error) {

    snapshotLastError =
      error.message;

    snapshotPending =
      false;
  }
}

/* =========================================================
   DEPTH WEBSOCKET
========================================================= */

function connectDepth() {

  if (
    depthWs
  ) {

    try {
      depthWs.close();
    } catch (_) {}
  }

  depthWs =
    new WebSocket(
      DEPTH_WS
    );

  depthWs.on(
    'open',
    () => {

      depthConnected =
        true;

      console.log(
        '[DEPTH] Connected'
      );

      requestSnapshot();
    }
  );

  depthWs.on(
    'message',
    raw => {

      try {

        const data =
          JSON.parse(
            raw.toString()
          );

        if (
          data.e ===
          'depthUpdate'
        ) {

          applyDepth(
            data
          );
        }

      } catch (error) {

        console.log(
          '[DEPTH MESSAGE ERROR]',
          error.message
        );
      }
    }
  );

  depthWs.on(
    'error',
    error => {

      console.log(
        '[DEPTH ERROR]',
        error.message
      );
    }
  );

  depthWs.on(
    'close',
    () => {

      depthConnected =
        false;

      setTimeout(
        connectDepth,
        5000
      );
    }
  );
}

/* =========================================================
   FUTURES KLINES
========================================================= */

function connectKlines() {

  if (
    klineWs
  ) {

    try {
      klineWs.close();
    } catch (_) {}
  }

  klineWs =
    new WebSocket(
      KLINE_WS
    );

  klineWs.on(
    'open',
    () => {

      klineConnected =
        true;

      console.log(
        '[KLINE] Connected'
      );
    }
  );

  klineWs.on(
    'message',
    raw => {

      try {

        const message =
          JSON.parse(
            raw.toString()
          );

        const data =
          message.data ||
          message;

        if (
          data.e !==
            'kline' ||
          !data.k
        ) {
          return;
        }

        const k =
          data.k;

        const tf =
          k.i;

        if (
          !TF[tf]
        ) {
          return;
        }

        const candle = {

          openTime:
            Number(k.t),

          open:
            Number(k.o),

          high:
            Number(k.h),

          low:
            Number(k.l),

          close:
            Number(k.c),

          volume:
            Number(k.v),

          closeTime:
            Number(k.T || 0),

          source:
            'futures'
        };

        const list =
          candles[tf];

        const index =
          list.findIndex(
            item =>
              item.openTime ===
              candle.openTime
          );

        if (
          index >= 0
        ) {

          list[index] =
            candle;

        } else {

          list.push(
            candle
          );
        }

        list.sort(
          (a, b) =>
            a.openTime -
            b.openTime
        );

        const max =
          TF[tf].limit +
          50;

        if (
          list.length >
          max
        ) {

          list.splice(
            0,
            list.length - max
          );
        }

        candleSource[tf] =
          'binance_futures_live';

        currentPrice =
          candle.close;

        updateMarketState();

        if (
          k.x === true
        ) {

          rebuildStructure();
        }

      } catch (error) {

        console.log(
          '[KLINE ERROR]',
          error.message
        );
      }
    }
  );

  klineWs.on(
    'error',
    error => {

      console.log(
        '[KLINE ERROR]',
        error.message
      );
    }
  );

  klineWs.on(
    'close',
    () => {

      klineConnected =
        false;

      setTimeout(
        connectKlines,
        5000
      );
    }
  );
}

/* =========================================================
   RAW ORDER BOOK
========================================================= */

function rawBook() {

  const largeBids = [];
  const largeAsks = [];

  for (
    const [
      priceText,
      quantity
    ] of bids
  ) {

    const price =
      Number(priceText);

    const usd =
      price * quantity;

    if (
      usd >= 50000
    ) {

      largeBids.push({
        price,
        quantity,
        usd
      });
    }
  }

  for (
    const [
      priceText,
      quantity
    ] of asks
  ) {

    const price =
      Number(priceText);

    const usd =
      price * quantity;

    if (
      usd >= 50000
    ) {

      largeAsks.push({
        price,
        quantity,
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
   LIQUIDITY MAP
========================================================= */

function liquidityMap() {

  const bsl =
    structure.majorBSL
      .filter(
        level =>
          currentPrice == null ||
          level.price >
            currentPrice
      )
      .sort(
        (a, b) =>
          a.price - b.price
      );

  const ssl =
    structure.majorSSL
      .filter(
        level =>
          currentPrice == null ||
          level.price <
            currentPrice
      )
      .sort(
        (a, b) =>
          b.price - a.price
      );

  return {

    buySideLiquidity:
      bsl.map(
        formatLevel
      ),

    sellSideLiquidity:
      ssl.map(
        formatLevel
      ),

    majorResistance:
      bsl
        .slice(
          0,
          3
        )
        .map(
          formatLevel
        ),

    majorSupport:
      ssl
        .slice(
          0,
          3
        )
        .map(
          formatLevel
        ),

    nearestBSL:
      bsl.length
        ? [
            formatLevel(
              bsl[0]
            )
          ]
        : [],

    nearestSSL:
      ssl.length
        ? [
            formatLevel(
              ssl[0]
            )
          ]
        : []
  };
}

/* =========================================================
   ROOT
========================================================= */

app.get(
  '/',
  (req, res) => {

    res.json({

      ok: true,

      service:
        'Binance BTCUSDT Major Liquidity Relay V2',

      symbol:
        SYMBOL,

      endpoints: [
        '/health',
        '/structure',
        '/liquidity',
        '/book'
      ]
    });
  }
);

/* =========================================================
   HEALTH
========================================================= */

app.get(
  '/health',
  (req, res) => {

    res.json({

      ok: true,

      service:
        'Binance BTCUSDT Major Liquidity Relay V2',

      symbol:
        SYMBOL,

      currentPrice,

      historicalStructureSource:
        'Binance Spot',

      liveStructureSource:
        'Binance Futures WebSocket',

      liquiditySource:
        'Binance Futures Order Book',

      candleHistory:
        Object.fromEntries(
          Object.keys(TF)
            .map(
              tf => [
                tf,
                candles[tf].length
              ]
            )
        ),

      candleSource,

      candleStats,

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

      marketState,

      orderBook: {

        initialized:
          bookInitialized,

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
          pending.length,

        snapshotPending,

        snapshotId:
          depthLastUpdateId,

        snapshotRequests,

        snapshot429s,

        lastSnapshotResponse,

        snapshotLastStatus,

        snapshotLastError
      },

      updatedAt:
        iso()
    });
  }
);

/* =========================================================
   STRUCTURE
========================================================= */

app.get(
  '/structure',
  (req, res) => {

    res.json({

      ok: true,

      symbol:
        SYMBOL,

      currentPrice,

      historicalSource:
        'Binance Spot',

      liveSource:
        'Binance Futures',

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

      marketState,

      updatedAt:
        structure.updatedAt
    });
  }
);

/* =========================================================
   LIQUIDITY
========================================================= */

app.get(
  '/liquidity',
  (req, res) => {

    res.json({

      ok: true,

      symbol:
        SYMBOL,

      currentPrice,

      marketStructure:
        liquidityMap(),

      marketState,

      rawRestingLiquidity:
        rawBook(),

      structureStats: {

        candleSource,

        candleHistory:
          Object.fromEntries(
            Object.keys(TF)
              .map(
                tf => [
                  tf,
                  candles[tf].length
                ]
              )
          ),

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
        iso()
    });
  }
);

/* =========================================================
   BOOK
========================================================= */

app.get(
  '/book',
  (req, res) => {

    const bidList =
      [...bids.entries()]
        .map(
          ([price, quantity]) => ({
            price:
              Number(price),

            quantity,

            usd:
              Number(price) *
              quantity
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

    const askList =
      [...asks.entries()]
        .map(
          ([price, quantity]) => ({
            price:
              Number(price),

            quantity,

            usd:
              Number(price) *
              quantity
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

      initialized:
        bookInitialized,

      lastUpdateId:
        depthLastUpdateId,

      bids:
        bidList,

      asks:
        askList,

      updatedAt:
        iso()
    });
  }
);

/* =========================================================
   RECOVERY
========================================================= */

setInterval(
  () => {

    if (
      gapRecovery
    ) {

      requestSnapshot();
    }

  },
  5000
);

/* =========================================================
   STRUCTURE REFRESH
========================================================= */

setInterval(
  () => {

    rebuildStructure();

  },
  30000
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

    await loadAllHistory();

    connectDepth();

    connectKlines();
  }
);
