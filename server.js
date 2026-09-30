const express = require('express');
const WebSocket = require('ws');

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 10000;
const SYMBOL = 'BTCUSDT';

const SPOT_API = 'https://data-api.binance.vision';

const FUTURES_DEPTH_WS =
  'wss://fstream.binance.com/ws/btcusdt@depth@100ms';

const FUTURES_KLINE_WS =
  'wss://fstream.binance.com/stream?streams=btcusdt@kline_15m/btcusdt@kline_1h/btcusdt@kline_4h/btcusdt@kline_1d';

const FUTURES_SNAPSHOT_WS =
  'wss://ws-fapi.binance.com/ws-fapi/v1';

/* ============================================================
   STRUCTURE SETTINGS
============================================================ */

const STRUCTURE = {
  '15m': {
    interval: '15m',
    weight: 1,
    candles: 300,
    pivotLeft: 3,
    pivotRight: 3,
    equalTolerancePct: 0.0015
  },

  '1h': {
    interval: '1h',
    weight: 2,
    candles: 300,
    pivotLeft: 4,
    pivotRight: 4,
    equalTolerancePct: 0.0015
  },

  '4h': {
    interval: '4h',
    weight: 3,
    candles: 250,
    pivotLeft: 5,
    pivotRight: 5,
    equalTolerancePct: 0.002
  },

  '1d': {
    interval: '1d',
    weight: 4,
    candles: 180,
    pivotLeft: 5,
    pivotRight: 5,
    equalTolerancePct: 0.0025
  }
};

/* ============================================================
   LIQUIDITY INTELLIGENCE SETTINGS
============================================================ */

const SETTINGS = {
  rawBookMinUsd: 50000,

  restingConfirmUsd: 250000,

  restingRadiusUsd: 35,

  levelZonePct: 0.0015,

  mergeDistanceUsd: 35,

  reactionMovePct: 0.003,

  minMajorScore: 40,

  maxMajorLevels: 8,

  intelligenceLevels: 5,

  orderClusterGapUsd: 35,

  orderClusterMinUsd: 500000,

  sweepLookbackMs: 15 * 60 * 1000
};

/* ============================================================
   DATA
============================================================ */

const candles = {
  '15m': [],
  '1h': [],
  '4h': [],
  '1d': []
};

const book = {
  bids: new Map(),
  asks: new Map()
};

const structure = {
  swingHighs: [],
  swingLows: [],
  equalHighs: [],
  equalLows: [],
  levels: []
};

const state = {
  currentPrice: null,

  depthConnected: false,
  snapshotConnected: false,
  klinesConnected: false,

  bookInitialized: false,

  lastUpdateId: 0,

  pendingEvents: [],

  gapRecovery: false,
  gapDetected: false,
  gapCount: 0,

  lastGapLocal: null,
  lastGapIncoming: null,

  snapshotId: null,
  snapshotRequests: 0,
  snapshot429s: 0,

  snapshotLastStatus: null,
  snapshotLastError: null,
  lastSnapshotResponse: null,

  updatedAt: null,

  candleStats: {
    '15m': {
      requests: 0,
      success: 0,
      errors: 0
    },

    '1h': {
      requests: 0,
      success: 0,
      errors: 0
    },

    '4h': {
      requests: 0,
      success: 0,
      errors: 0
    },

    '1d': {
      requests: 0,
      success: 0,
      errors: 0
    }
  },

  candleLastStatus: null,
  candleLastError: null,

  lastDepthMessage: null,
  lastKlineMessage: null,

  sweepEvents: []
};

/* ============================================================
   HELPERS
============================================================ */

function now() {
  return Date.now();
}

function pctDistance(price, current) {
  if (!current) return null;

  return ((price - current) / current) * 100;
}

function round(value, decimals = 2) {
  return Number(Number(value).toFixed(decimals));
}

/* ============================================================
   HTTP
============================================================ */

async function getJson(url) {
  const response = await fetch(url, {
    headers: {
      'User-Agent': 'Binance-Major-Liquidity-Relay/1.0'
    }
  });

  const text = await response.text();

  let data;

  try {
    data = JSON.parse(text);
  } catch (_) {
    throw new Error(
      `HTTP ${response.status}: non-JSON response`
    );
  }

  if (!response.ok) {
    throw new Error(
      `HTTP ${response.status}: ${JSON.stringify(data).slice(0, 500)}`
    );
  }

  return data;
}

/* ============================================================
   CANDLE CONVERSION
============================================================ */

function candleFromArray(row) {
  return {
    openTime: Number(row[0]),
    open: Number(row[1]),
    high: Number(row[2]),
    low: Number(row[3]),
    close: Number(row[4]),
    volume: Number(row[5]),
    closeTime: Number(row[6])
  };
}

/* ============================================================
   HISTORICAL SPOT CANDLES
============================================================ */

async function loadHistoricalCandles() {
  for (const [tf, cfg] of Object.entries(STRUCTURE)) {
    state.candleStats[tf].requests += 1;

    try {
      const url =
        `${SPOT_API}/api/v3/klines` +
        `?symbol=${SYMBOL}` +
        `&interval=${cfg.interval}` +
        `&limit=${cfg.candles}`;

      const data = await getJson(url);

      candles[tf] = Array.isArray(data)
        ? data.map(candleFromArray)
        : [];

      state.candleStats[tf].success += 1;

      state.candleLastStatus = 200;
      state.candleLastError = null;
    } catch (error) {
      state.candleStats[tf].errors += 1;

      const match =
        String(error.message).match(/HTTP (\d+)/);

      state.candleLastStatus =
        match ? Number(match[1]) : null;

      state.candleLastError = error.message;
    }
  }

  rebuildStructure();
}

/* ============================================================
   PIVOT DETECTION
============================================================ */

function isPivotHigh(data, i, left, right) {
  if (
    i < left ||
    i + right >= data.length
  ) {
    return false;
  }

  const high = data[i].high;

  for (
    let j = i - left;
    j <= i + right;
    j++
  ) {
    if (j !== i && data[j].high >= high) {
      return false;
    }
  }

  return true;
}

function isPivotLow(data, i, left, right) {
  if (
    i < left ||
    i + right >= data.length
  ) {
    return false;
  }

  const low = data[i].low;

  for (
    let j = i - left;
    j <= i + right;
    j++
  ) {
    if (j !== i && data[j].low <= low) {
      return false;
    }
  }

  return true;
}

/* ============================================================
   REACTION COUNT
============================================================ */

function reactionCount(
  data,
  price,
  side,
  tolerancePct
) {
  let reactions = 0;

  for (let i = 1; i < data.length; i++) {
    const candle = data[i];

    const near =
      side === 'BSL'
        ? Math.abs(candle.high - price) / price <= tolerancePct
        : Math.abs(candle.low - price) / price <= tolerancePct;

    if (!near) continue;

    const previous =
      data.slice(
        Math.max(0, i - 8),
        i
      );

    if (!previous.length) continue;

    const base =
      side === 'BSL'
        ? Math.max(
            ...previous.map(x => x.high)
          )
        : Math.min(
            ...previous.map(x => x.low)
          );

    const move =
      side === 'BSL'
        ? (base - candle.close) / price
        : (candle.close - base) / price;

    if (move >= SETTINGS.reactionMovePct) {
      reactions += 1;
    }
  }

  return reactions;
}

/* ============================================================
   BUILD TIMEFRAME STRUCTURE
============================================================ */

function buildTfStructure(tf) {
  const cfg = STRUCTURE[tf];
  const data = candles[tf];

  const highs = [];
  const lows = [];

  if (
    !data ||
    data.length <
      cfg.pivotLeft +
        cfg.pivotRight +
        5
  ) {
    return {
      highs,
      lows
    };
  }

  for (
    let i = cfg.pivotLeft;
    i < data.length - cfg.pivotRight;
    i++
  ) {
    if (
      isPivotHigh(
        data,
        i,
        cfg.pivotLeft,
        cfg.pivotRight
      )
    ) {
      highs.push({
        price: data[i].high,
        time: data[i].openTime,
        tf
      });
    }

    if (
      isPivotLow(
        data,
        i,
        cfg.pivotLeft,
        cfg.pivotRight
      )
    ) {
      lows.push({
        price: data[i].low,
        time: data[i].openTime,
        tf
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

function findEqualLevels(
  points,
  tolerancePct,
  side
) {
  const out = [];

  for (let i = 0; i < points.length; i++) {
    const cluster = [points[i]];

    for (
      let j = i + 1;
      j < points.length;
      j++
    ) {
      const reference =
        cluster.reduce(
          (sum, p) => sum + p.price,
          0
        ) / cluster.length;

      if (
        Math.abs(
          points[j].price - reference
        ) /
          reference <=
        tolerancePct
      ) {
        cluster.push(points[j]);
      }
    }

    if (cluster.length >= 2) {
      const average =
        cluster.reduce(
          (sum, p) => sum + p.price,
          0
        ) / cluster.length;

      out.push({
        side,
        price: average,
        count: cluster.length,
        timeframes: [
          ...new Set(
            cluster.map(x => x.tf)
          )
        ]
      });
    }
  }

  return out;
}

/* ============================================================
   MERGE STRUCTURAL POINTS
============================================================ */

function mergeStructuralPoints(
  allPoints,
  side
) {
  const sorted = [...allPoints].sort(
    (a, b) => a.price - b.price
  );

  const clusters = [];

  for (const point of sorted) {
    const existing = clusters.find(
      cluster =>
        Math.abs(
          point.price - cluster.price
        ) <=
        Math.max(
          SETTINGS.mergeDistanceUsd,
          cluster.price *
            SETTINGS.levelZonePct
        )
    );

    if (existing) {
      existing.points.push(point);

      existing.price =
        existing.points.reduce(
          (sum, p) => sum + p.price,
          0
        ) /
        existing.points.length;
    } else {
      clusters.push({
        price: point.price,
        points: [point]
      });
    }
  }

  return clusters.map(cluster => {
    const timeframes = [
      ...new Set(
        cluster.points.map(
          point => point.tf
        )
      )
    ];

    const equalCount =
      cluster.points.reduce(
        (sum, point) =>
          sum +
          (point.equalCount || 1),
        0
      );

    const reactions =
      cluster.points.reduce(
        (sum, point) =>
          sum +
          (point.reactions || 0),
        0
      );

    const timeframeScore =
      timeframes.reduce(
        (sum, tf) =>
          sum + STRUCTURE[tf].weight,
        0
      );

    const score = Math.round(
      timeframeScore * 10 +
        Math.min(equalCount, 12) * 5 +
        Math.min(reactions, 30) * 2
    );

    const prices =
      cluster.points.map(
        point => point.price
      );

    return {
      side,

      price: cluster.price,

      priceLow: Math.min(...prices),

      priceHigh: Math.max(...prices),

      score,

      timeframes,

      equalCount,

      reactions,

      restingUsd: 0,

      restingLevels: [],

      distancePct:
        state.currentPrice
          ? pctDistance(
              cluster.price,
              state.currentPrice
            )
          : null,

      source:
        'spot_structure+futures_liquidity'
    };
  });
}

/* ============================================================
   REBUILD STRUCTURE
============================================================ */

function rebuildStructure() {
  const allHighs = [];
  const allLows = [];

  const equalHighs = [];
  const equalLows = [];

  for (const tf of Object.keys(STRUCTURE)) {
    const tfStructure =
      buildTfStructure(tf);

    allHighs.push(
      ...tfStructure.highs
    );

    allLows.push(
      ...tfStructure.lows
    );

    const cfg = STRUCTURE[tf];

    const eh =
      findEqualLevels(
        tfStructure.highs,
        cfg.equalTolerancePct,
        'BSL'
      );

    const el =
      findEqualLevels(
        tfStructure.lows,
        cfg.equalTolerancePct,
        'SSL'
      );

    equalHighs.push(
      ...eh.map(x => ({
        ...x,
        tf
      }))
    );

    equalLows.push(
      ...el.map(x => ({
        ...x,
        tf
      }))
    );
  }

  const highsWithMetrics =
    allHighs.map(point => ({
      ...point,

      equalCount:
        equalHighs
          .filter(
            level =>
              Math.abs(
                level.price -
                  point.price
              ) /
                point.price <=
              0.003
          )
          .reduce(
            (max, level) =>
              Math.max(
                max,
                level.count
              ),
            1
          ),

      reactions:
        reactionCount(
          candles[point.tf],
          point.price,
          'BSL',
          STRUCTURE[
            point.tf
          ].equalTolerancePct
        )
    }));

  const lowsWithMetrics =
    allLows.map(point => ({
      ...point,

      equalCount:
        equalLows
          .filter(
            level =>
              Math.abs(
                level.price -
                  point.price
              ) /
                point.price <=
              0.003
          )
          .reduce(
            (max, level) =>
              Math.max(
                max,
                level.count
              ),
            1
          ),

      reactions:
        reactionCount(
          candles[point.tf],
          point.price,
          'SSL',
          STRUCTURE[
            point.tf
          ].equalTolerancePct
        )
    }));

  structure.swingHighs =
    highsWithMetrics;

  structure.swingLows =
    lowsWithMetrics;

  structure.equalHighs =
    equalHighs;

  structure.equalLows =
    equalLows;

  structure.levels = [
    ...mergeStructuralPoints(
      highsWithMetrics,
      'BSL'
    ),

    ...mergeStructuralPoints(
      lowsWithMetrics,
      'SSL'
    )
  ];

  applyRestingLiquidity();
}

/* ============================================================
   ORDER BOOK
============================================================ */

function getBookRows(side) {
  const map =
    side === 'BSL'
      ? book.asks
      : book.bids;

  const rows = [];

  for (const [
    price,
    quantity
  ] of map.entries()) {
    const value =
      Number(price) *
      Number(quantity);

    if (
      value >=
      SETTINGS.rawBookMinUsd
    ) {
      rows.push({
        price: Number(price),
        quantity: Number(quantity),
        usd: value
      });
    }
  }

  rows.sort((a, b) =>
    side === 'BSL'
      ? a.price - b.price
      : b.price - a.price
  );

  return rows;
}

/* ============================================================
   RESTING LIQUIDITY NEAR STRUCTURE
============================================================ */

function restingNear(
  levelPrice,
  side
) {
  const map =
    side === 'BSL'
      ? book.asks
      : book.bids;

  let total = 0;

  const levels = [];

  for (const [
    priceKey,
    quantity
  ] of map.entries()) {
    const price =
      Number(priceKey);

    if (
      Math.abs(
        price - levelPrice
      ) <=
      SETTINGS.restingRadiusUsd
    ) {
      const value =
        price *
        Number(quantity);

      if (
        value >=
        SETTINGS.rawBookMinUsd
      ) {
        total += value;

        levels.push({
          price,
          quantity: Number(quantity),
          usd: value
        });
      }
    }
  }

  levels.sort(
    (a, b) => b.usd - a.usd
  );

  return {
    total,
    levels: levels.slice(0, 10)
  };
}

/* ============================================================
   APPLY ORDER BOOK CONFIRMATION
============================================================ */

function applyRestingLiquidity() {
  for (const level of structure.levels) {
    const resting =
      restingNear(
        level.price,
        level.side
      );

    level.restingUsd =
      round(resting.total, 2);

    level.restingLevels =
      resting.levels;

    if (
      resting.total >=
      SETTINGS.restingConfirmUsd
    ) {
      level.score += Math.min(
        25,
        Math.floor(
          resting.total / 250000
        ) * 5
      );
    }

    level.distancePct =
      state.currentPrice
        ? round(
            pctDistance(
              level.price,
              state.currentPrice
            ),
            3
          )
        : null;
  }
}

/* ============================================================
   FILTER STRUCTURAL LEVELS
============================================================ */

function filteredLevels(side) {
  return structure.levels
    .filter(
      level =>
        level.side === side &&
        level.score >=
          SETTINGS.minMajorScore
    )
    .sort((a, b) => {
      const distanceA =
        state.currentPrice
          ? Math.abs(
              a.price -
                state.currentPrice
            )
          : Infinity;

      const distanceB =
        state.currentPrice
          ? Math.abs(
              b.price -
                state.currentPrice
            )
          : Infinity;

      return distanceA - distanceB;
    });
}

/* ============================================================
   LEVEL CLUSTERING
============================================================ */

function clusterLevels(levels) {
  const sorted =
    [...levels].sort(
      (a, b) =>
        a.price - b.price
    );

  const result = [];

  for (const level of sorted) {
    const last =
      result[result.length - 1];

    if (
      last &&
      Math.abs(
        level.price -
          last.priceHigh
      ) <=
        Math.max(
          SETTINGS.mergeDistanceUsd,
          last.price *
            SETTINGS.levelZonePct
        )
    ) {
      last.members.push(level);

      last.priceLow =
        Math.min(
          last.priceLow,
          level.priceLow
        );

      last.priceHigh =
        Math.max(
          last.priceHigh,
          level.priceHigh
        );

      last.price =
        last.members.reduce(
          (sum, x) =>
            sum + x.price,
          0
        ) /
        last.members.length;

      last.score =
        Math.max(
          ...last.members.map(
            x => x.score
          )
        );

      last.equalCount =
        Math.max(
          ...last.members.map(
            x => x.equalCount
          )
        );

      last.reactions =
        Math.max(
          ...last.members.map(
            x => x.reactions
          )
        );

      last.timeframes = [
        ...new Set(
          last.members.flatMap(
            x => x.timeframes
          )
        )
      ];

      last.restingUsd =
        last.members.reduce(
          (sum, x) =>
            sum + x.restingUsd,
          0
        );
    } else {
      result.push({
        ...level,

        members: [level],

        priceLow:
          level.priceLow,

        priceHigh:
          level.priceHigh
      });
    }
  }

  return result;
}

/* ============================================================
   STRENGTH
============================================================ */

function strength(level) {
  const timeframeCount =
    level.timeframes.length;

  if (
    level.score >= 125 ||
    (
      timeframeCount >= 3 &&
      level.equalCount >= 4
    )
  ) {
    return 'HIGH';
  }

  if (
    level.score >= 90 ||
    timeframeCount >= 2 ||
    level.equalCount >= 3
  ) {
    return 'MEDIUM';
  }

  return 'LOW';
}

/* ============================================================
   DECORATE LEVEL
============================================================ */

function decorateLevel(level) {
  return {
    side: level.side,

    price: round(
      level.price,
      2
    ),

    priceLow: round(
      level.priceLow,
      2
    ),

    priceHigh: round(
      level.priceHigh,
      2
    ),

    score: level.score,

    strength:
      strength(level),

    timeframes:
      level.timeframes,

    equalCount:
      level.equalCount,

    reactions:
      level.reactions,

    restingUsd:
      round(
        level.restingUsd,
        2
      ),

    restingConfirmed:
      level.restingUsd >=
      SETTINGS.restingConfirmUsd,

    restingLevels:
      level.restingLevels,

    distancePct:
      state.currentPrice
        ? round(
            pctDistance(
              level.price,
              state.currentPrice
            ),
            3
          )
        : null
  };
}

/* ============================================================
   ORDER BOOK CLUSTERS
============================================================ */

function clusterBookRows(
  rows,
  side
) {
  const output = [];

  for (const row of rows) {
    const last =
      output[
        output.length - 1
      ];

    if (
      last &&
      Math.abs(
        row.price -
          last.priceHigh
      ) <=
        SETTINGS.orderClusterGapUsd
    ) {
      last.priceLow =
        Math.min(
          last.priceLow,
          row.price
        );

      last.priceHigh =
        Math.max(
          last.priceHigh,
          row.price
        );

      last.usd += row.usd;

      last.levels += 1;
    } else {
      output.push({
        side,

        priceLow:
          row.price,

        priceHigh:
          row.price,

        midpoint:
          row.price,

        usd:
          row.usd,

        levels: 1
      });
    }
  }

  return output
    .filter(
      x =>
        x.usd >=
        SETTINGS.orderClusterMinUsd
    )
    .map(x => ({
      ...x,

      midpoint: round(
        (
          x.priceLow +
          x.priceHigh
        ) / 2,
        2
      ),

      usd: round(
        x.usd,
        2
      )
    }));
}

function orderBookClusters() {
  const bids =
    clusterBookRows(
      getBookRows('SSL'),
      'bid'
    );

  const asks =
    clusterBookRows(
      getBookRows('BSL'),
      'ask'
    );

  return {
    bids: bids.slice(0, 5),
    asks: asks.slice(0, 5)
  };
}

/* ============================================================
   LIQUIDITY SWEEPS
============================================================ */

function recordSweep(
  side,
  level,
  price
) {
  const event = {
    side,

    level: round(
      level,
      2
    ),

    price: round(
      price,
      2
    ),

    time:
      new Date().toISOString()
  };

  const last =
    state.sweepEvents[
      state.sweepEvents.length - 1
    ];

  if (
    last &&
    last.side === side &&
    Math.abs(
      last.level - level
    ) < 20 &&
    now() -
      new Date(
        last.time
      ).getTime() <
      SETTINGS.sweepLookbackMs
  ) {
    return;
  }

  state.sweepEvents.push(
    event
  );

  if (
    state.sweepEvents.length >
    30
  ) {
    state.sweepEvents.shift();
  }
}

function detectSweep(
  previousPrice,
  currentPrice
) {
  if (
    !previousPrice ||
    !currentPrice
  ) {
    return;
  }

  for (const level of structure.levels) {
    if (
      level.side === 'BSL' &&
      previousPrice <
        level.price &&
      currentPrice >=
        level.price
    ) {
      recordSweep(
        'BSL',
        level.price,
        currentPrice
      );
    }

    if (
      level.side === 'SSL' &&
      previousPrice >
        level.price &&
      currentPrice <=
        level.price
    ) {
      recordSweep(
        'SSL',
        level.price,
        currentPrice
      );
    }
  }
}

/* ============================================================
   PRICE
============================================================ */

function updatePrice(price) {
  const p = Number(price);

  if (!Number.isFinite(p)) {
    return;
  }

  const previous =
    state.currentPrice;

  state.currentPrice = p;

  detectSweep(
    previous,
    p
  );

  applyRestingLiquidity();

  state.updatedAt =
    new Date().toISOString();
}

/* ============================================================
   LIQUIDITY INTELLIGENCE
============================================================ */

function intelligence() {
  applyRestingLiquidity();

  let bsl =
    clusterLevels(
      filteredLevels('BSL')
    ).filter(
      level =>
        level.price >
        (state.currentPrice || 0)
    );

  let ssl =
    clusterLevels(
      filteredLevels('SSL')
    ).filter(
      level =>
        level.price <
        (
          state.currentPrice ||
          Infinity
        )
    );

  bsl.sort(
    (a, b) =>
      a.price - b.price
  );

  ssl.sort(
    (a, b) =>
      b.price - a.price
  );

  const nearestBSL =
    bsl[0] || null;

  const nearestSSL =
    ssl[0] || null;

  const nextBSL =
    bsl
      .slice(
        0,
        SETTINGS.intelligenceLevels
      )
      .map(decorateLevel);

  const nextSSL =
    ssl
      .slice(
        0,
        SETTINGS.intelligenceLevels
      )
      .map(decorateLevel);

  let stateName =
    'NO STRUCTURAL LIQUIDITY';

  if (
    nearestBSL &&
    nearestSSL
  ) {
    stateName =
      'BETWEEN LIQUIDITY';
  } else if (nearestBSL) {
    stateName =
      'UPSIDE LIQUIDITY ONLY';
  } else if (nearestSSL) {
    stateName =
      'DOWNSIDE LIQUIDITY ONLY';
  }

  return {
    state:
      stateName,

    nearestBSL:
      nearestBSL
        ? decorateLevel(
            nearestBSL
          )
        : null,

    nearestSSL:
      nearestSSL
        ? decorateLevel(
            nearestSSL
          )
        : null,

    nextBSL,

    nextSSL,

    orderBook:
      orderBookClusters(),

    recentSweeps:
      state.sweepEvents
        .slice(-10)
        .reverse()
  };
}

/* ============================================================
   SNAPSHOT
============================================================ */

function setBookSnapshot(
  snapshot
) {
  book.bids.clear();
  book.asks.clear();

  for (
    const [
      price,
      quantity
    ] of snapshot.bids || []
  ) {
    if (
      Number(quantity) > 0
    ) {
      book.bids.set(
        Number(price),
        Number(quantity)
      );
    }
  }

  for (
    const [
      price,
      quantity
    ] of snapshot.asks || []
  ) {
    if (
      Number(quantity) > 0
    ) {
      book.asks.set(
        Number(price),
        Number(quantity)
      );
    }
  }

  state.bookInitialized =
    true;

  state.lastUpdateId =
    Number(
      snapshot.lastUpdateId ||
      0
    );

  state.snapshotId =
    state.lastUpdateId;

  state.pendingEvents = [];

  state.gapRecovery =
    false;

  state.gapDetected =
    false;

  state.updatedAt =
    new Date().toISOString();

  applyRestingLiquidity();
}

/* ============================================================
   APPLY DEPTH EVENT
============================================================ */

function applyDepthEvent(
  msg
) {
  if (
    !msg ||
    !Array.isArray(msg.b) ||
    !Array.isArray(msg.a)
  ) {
    return;
  }

  for (
    const [
      price,
      quantity
    ] of msg.b
  ) {
    const p = Number(price);
    const q = Number(quantity);

    if (q === 0) {
      book.bids.delete(p);
    } else {
      book.bids.set(
        p,
        q
      );
    }
  }

  for (
    const [
      price,
      quantity
    ] of msg.a
  ) {
    const p = Number(price);
    const q = Number(quantity);

    if (q === 0) {
      book.asks.delete(p);
    } else {
      book.asks.set(
        p,
        q
      );
    }
  }

  state.lastUpdateId =
    Number(
      msg.u ||
      state.lastUpdateId
    );

  state.lastDepthMessage =
    new Date().toISOString();

  state.updatedAt =
    state.lastDepthMessage;
}

/* ============================================================
   DEPTH EVENT HANDLER
============================================================ */

function handleDepthMessage(
  msg
) {
  const U = Number(msg.U);
  const u = Number(msg.u);

  if (
    !Number.isFinite(U) ||
    !Number.isFinite(u)
  ) {
    return;
  }

  if (
    !state.bookInitialized
  ) {
    if (
      state.pendingEvents.length <
      5000
    ) {
      state.pendingEvents.push(
        msg
      );
    }

    return;
  }

  if (
    u <=
    state.lastUpdateId
  ) {
    return;
  }

  if (
    U >
    state.lastUpdateId + 1
  ) {
    state.gapDetected =
      true;

    state.gapRecovery =
      true;

    state.gapCount += 1;

    state.lastGapLocal =
      state.lastUpdateId;

    state.lastGapIncoming =
      U;

    state.bookInitialized =
      false;

    state.pendingEvents = [
      msg
    ];

    requestSnapshot();

    return;
  }

  applyDepthEvent(msg);
}

/* ============================================================
   SNAPSHOT REQUEST
============================================================ */

let snapshotBusy = false;
let snapshotLastAt = 0;

function requestSnapshot() {
  if (snapshotBusy) {
    return;
  }

  if (
    now() -
      snapshotLastAt <
    65000
  ) {
    return;
  }

  snapshotBusy = true;
  snapshotLastAt = now();

  state.snapshotRequests +=
    1;

  let ws;

  try {
    ws =
      new WebSocket(
        FUTURES_SNAPSHOT_WS
      );

    const requestId =
      `depth-${Date.now()}`;

    const timer =
      setTimeout(() => {
        try {
          ws.close();
        } catch (_) {}
      }, 15000);

    ws.on(
      'open',
      () => {
        state.snapshotConnected =
          true;

        ws.send(
          JSON.stringify({
            id: requestId,

            method: 'depth',

            params: {
              symbol: SYMBOL,
              limit: 1000
            }
          })
        );
      }
    );

    ws.on(
      'message',
      raw => {
        try {
          const data =
            JSON.parse(
              raw.toString()
            );

          if (
            data.id !==
              requestId ||
            !data.result
          ) {
            return;
          }

          clearTimeout(
            timer
          );

          setBookSnapshot(
            data.result
          );

          state.snapshotLastStatus =
            200;

          state.snapshotLastError =
            null;

          state.lastSnapshotResponse =
            new Date().toISOString();

          const pending =
            [
              ...state.pendingEvents
            ].sort(
              (a, b) =>
                Number(a.U) -
                Number(b.U)
            );

          state.pendingEvents =
            [];

          let bridged =
            false;

          for (
            const event of pending
          ) {
            const U =
              Number(event.U);

            const u =
              Number(event.u);

            if (
              u <=
              state.lastUpdateId
            ) {
              continue;
            }

            if (
              U <=
                state.lastUpdateId +
                  1 &&
              u >=
                state.lastUpdateId +
                  1
            ) {
              applyDepthEvent(
                event
              );

              bridged =
                true;

              continue;
            }

            if (
              bridged &&
              U <=
                state.lastUpdateId +
                  1
            ) {
              applyDepthEvent(
                event
              );

              continue;
            }

            if (
              U >
              state.lastUpdateId +
                1
            ) {
              state.bookInitialized =
                false;

              state.gapRecovery =
                true;

              state.pendingEvents.push(
                event
              );

              break;
            }
          }

          state.updatedAt =
            new Date().toISOString();

          try {
            ws.close();
          } catch (_) {}
        } catch (error) {
          state.snapshotLastError =
            error.message;
        }
      }
    );

    ws.on(
      'error',
      error => {
        state.snapshotLastError =
          error.message;
      }
    );

    ws.on(
      'close',
      () => {
        state.snapshotConnected =
          false;
      }
    );
  } catch (error) {
    state.snapshotLastError =
      error.message;
  } finally {
    setTimeout(() => {
      snapshotBusy = false;
    }, 1000);
  }
}

/* ============================================================
   DEPTH CONNECTION
============================================================ */

function connectDepth() {
  const ws =
    new WebSocket(
      FUTURES_DEPTH_WS
    );

  state.depthConnected =
    false;

  ws.on(
    'open',
    () => {
      state.depthConnected =
        true;

      state.updatedAt =
        new Date().toISOString();

      requestSnapshot();
    }
  );

  ws.on(
    'message',
    raw => {
      try {
        const msg =
          JSON.parse(
            raw.toString()
          );

        if (
          msg.e ===
          'depthUpdate'
        ) {
          handleDepthMessage(
            msg
          );
        }
      } catch (_) {}
    }
  );

  ws.on(
    'close',
    () => {
      state.depthConnected =
        false;

      setTimeout(
        connectDepth,
        3000
      );
    }
  );

  ws.on(
    'error',
    () => {}
  );
}

/* ============================================================
   FUTURES KLINE CONNECTION
============================================================ */

function connectKlines() {
  const ws =
    new WebSocket(
      FUTURES_KLINE_WS
    );

  state.klinesConnected =
    false;

  ws.on(
    'open',
    () => {
      state.klinesConnected =
        true;
    }
  );

  ws.on(
    'message',
    raw => {
      try {
        const wrapper =
          JSON.parse(
            raw.toString()
          );

        const k =
          wrapper.data?.k;

        if (!k) {
          return;
        }

        const tf =
          String(k.i);

        if (!candles[tf]) {
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
            Number(k.T)
        };

        const arr =
          candles[tf];

        const index =
          arr.findIndex(
            x =>
              x.openTime ===
              candle.openTime
          );

        if (index >= 0) {
          arr[index] =
            candle;
        } else {
          arr.push(
            candle
          );

          if (
            arr.length >
            STRUCTURE[tf]
              .candles
          ) {
            arr.shift();
          }
        }

        updatePrice(
          candle.close
        );

        state.lastKlineMessage =
          new Date().toISOString();

        if (k.x) {
          rebuildStructure();
        }
      } catch (_) {}
    }
  );

  ws.on(
    'close',
    () => {
      state.klinesConnected =
        false;

      setTimeout(
        connectKlines,
        3000
      );
    }
  );

  ws.on(
    'error',
    () => {}
  );
}

/* ============================================================
   PUBLIC STRUCTURE
============================================================ */

function publicStructure() {
  applyRestingLiquidity();

  const bsl =
    filteredLevels('BSL')
      .sort(
        (a, b) =>
          a.price - b.price
      )
      .slice(
        0,
        SETTINGS.maxMajorLevels
      )
      .map(decorateLevel);

  const ssl =
    filteredLevels('SSL')
      .sort(
        (a, b) =>
          b.price - a.price
      )
      .slice(
        0,
        SETTINGS.maxMajorLevels
      )
      .map(decorateLevel);

  const resistance =
    bsl
      .filter(
        level =>
          level.price >
          (state.currentPrice || 0)
      )
      .slice(0, 3);

  const support =
    ssl
      .filter(
        level =>
          level.price <
          (
            state.currentPrice ||
            Infinity
          )
      )
      .slice(0, 3);

  return {
    buySideLiquidity: bsl,

    sellSideLiquidity: ssl,

    majorResistance:
      resistance,

    majorSupport:
      support,

    nearestBSL:
      resistance.slice(0, 1),

    nearestSSL:
      support.slice(0, 1)
  };
}

/* ============================================================
   RAW RESTING LIQUIDITY
============================================================ */

function rawRestingLiquidity() {
  return {
    largeBids:
      getBookRows('SSL')
        .sort(
          (a, b) =>
            b.usd - a.usd
        )
        .slice(0, 25),

    largeAsks:
      getBookRows('BSL')
        .sort(
          (a, b) =>
            b.usd - a.usd
        )
        .slice(0, 25)
  };
}

/* ============================================================
   HEALTH
============================================================ */

function health() {
  return {
    ok: true,

    service:
      'Binance BTCUSDT Major Liquidity Relay',

    symbol: SYMBOL,

    currentPrice:
      state.currentPrice,

    historicalStructureSource:
      'Binance Spot',

    liveStructureSource:
      'Binance Futures WebSocket',

    liquiditySource:
      'Binance Futures Order Book',

    candleHistory:
      Object.fromEntries(
        Object.entries(
          candles
        ).map(
          ([tf, arr]) => [
            tf,
            arr.length
          ]
        )
      ),

    candleSource:
      Object.fromEntries(
        Object.keys(
          STRUCTURE
        ).map(
          tf => [
            tf,
            'binance_spot'
          ]
        )
      ),

    candleStats:
      state.candleStats,

    candleLastStatus:
      state.candleLastStatus,

    candleLastError:
      state.candleLastError,

    structure: {
      swingHighs:
        structure.swingHighs
          .length,

      swingLows:
        structure.swingLows
          .length,

      equalHighs:
        structure.equalHighs
          .length,

      equalLows:
        structure.equalLows
          .length,

      majorBSL:
        filteredLevels(
          'BSL'
        ).length,

      majorSSL:
        filteredLevels(
          'SSL'
        ).length,

      updatedAt:
        state.updatedAt
    },

    orderBook: {
      initialized:
        state.bookInitialized,

      bids:
        book.bids.size,

      asks:
        book.asks.size,

      lastUpdateId:
        state.lastUpdateId
    },

    connections: {
      depth:
        state.depthConnected,

      snapshot:
        state.snapshotConnected,

      klines:
        state.klinesConnected
    },

    sync: {
      gapRecovery:
        state.gapRecovery,

      gapDetected:
        state.gapDetected,

      gapCount:
        state.gapCount,

      lastGapLocal:
        state.lastGapLocal,

      lastGapIncoming:
        state.lastGapIncoming,

      pendingEvents:
        state.pendingEvents.length,

      snapshotPending:
        snapshotBusy,

      snapshotId:
        state.snapshotId,

      snapshotRequests:
        state.snapshotRequests,

      snapshot429s:
        state.snapshot429s,

      lastSnapshotResponse:
        state.lastSnapshotResponse,

      snapshotLastStatus:
        state.snapshotLastStatus,

      snapshotLastError:
        state.snapshotLastError
    },

    updatedAt:
      state.updatedAt
  };
}

/* ============================================================
   API ROUTES
============================================================ */

app.get(
  '/',
  (_req, res) => {
    res.json(
      health()
    );
  }
);

app.get(
  '/health',
  (_req, res) => {
    res.json(
      health()
    );
  }
);

app.get(
  '/structure',
  (_req, res) => {
    res.json({
      ok: true,

      symbol: SYMBOL,

      currentPrice:
        state.currentPrice,

      marketStructure:
        publicStructure(),

      intelligence:
        intelligence(),

      structureStats: {
        candleSource:
          Object.fromEntries(
            Object.keys(
              STRUCTURE
            ).map(
              tf => [
                tf,
                'binance_spot'
              ]
            )
          ),

        candleHistory:
          Object.fromEntries(
            Object.entries(
              candles
            ).map(
              ([tf, arr]) => [
                tf,
                arr.length
              ]
            )
          ),

        swingHighs:
          structure.swingHighs
            .length,

        swingLows:
          structure.swingLows
            .length,

        equalHighs:
          structure.equalHighs
            .length,

        equalLows:
          structure.equalLows
            .length
      },

      updatedAt:
        state.updatedAt
    });
  }
);

app.get(
  '/liquidity',
  (_req, res) => {
    res.json({
      ok: true,

      symbol: SYMBOL,

      currentPrice:
        state.currentPrice,

      marketStructure:
        publicStructure(),

      intelligence:
        intelligence(),

      rawRestingLiquidity:
        rawRestingLiquidity(),

      structureStats: {
        candleSource:
          Object.fromEntries(
            Object.keys(
              STRUCTURE
            ).map(
              tf => [
                tf,
                'binance_spot'
              ]
            )
          ),

        candleHistory:
          Object.fromEntries(
            Object.entries(
              candles
            ).map(
              ([tf, arr]) => [
                tf,
                arr.length
              ]
            )
          ),

        swingHighs:
          structure.swingHighs
            .length,

        swingLows:
          structure.swingLows
            .length,

        equalHighs:
          structure.equalHighs
            .length,

        equalLows:
          structure.equalLows
            .length
      },

      updatedAt:
        state.updatedAt
    });
  }
);

app.get(
  '/intelligence',
  (_req, res) => {
    res.json({
      ok: true,

      symbol: SYMBOL,

      currentPrice:
        state.currentPrice,

      intelligence:
        intelligence(),

      updatedAt:
        state.updatedAt
    });
  }
);

app.get(
  '/book',
  (_req, res) => {
    res.json({
      ok: true,

      symbol: SYMBOL,

      currentPrice:
        state.currentPrice,

      initialized:
        state.bookInitialized,

      lastUpdateId:
        state.lastUpdateId,

      bids:
        [
          ...book.bids.entries()
        ].slice(0, 1000),

      asks:
        [
          ...book.asks.entries()
        ].slice(0, 1000)
    });
  }
);

/* ============================================================
   MAINTENANCE
============================================================ */

setInterval(
  () => {
    if (
      !state.bookInitialized &&
      state.depthConnected
    ) {
      requestSnapshot();
    }

    if (
      state.bookInitialized
    ) {
      applyRestingLiquidity();
    }
  },
  5000
);

setInterval(
  () => {
    const ready =
      Object.values(
        candles
      ).every(
        arr => arr.length > 0
      );

    if (!ready) {
      return;
    }

    rebuildStructure();
  },
  60000
);

/* ============================================================
   START
============================================================ */

async function start() {
  await loadHistoricalCandles();

  connectDepth();

  connectKlines();

  app.listen(
    PORT,
    () => {
      console.log(
        `Binance Futures Major Liquidity Relay listening on ${PORT}`
      );
    }
  );
}

start().catch(
  error => {
    console.error(
      'STARTUP ERROR',
      error
    );

    process.exit(1);
  }
);
