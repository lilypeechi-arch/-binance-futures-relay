const express = require('express');
const WebSocket = require('ws');

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 10000;
const SYMBOL = 'BTCUSDT';

const SPOT_API = 'https://data-api.binance.vision';

const DEPTH_WS =
  'wss://fstream.binance.com/ws/btcusdt@depth@100ms';

const KLINE_WS =
  'wss://fstream.binance.com/stream?streams=btcusdt@kline_15m/btcusdt@kline_1h/btcusdt@kline_4h/btcusdt@kline_1d';

const SNAPSHOT_WS =
  'wss://ws-fapi.binance.com/ws-fapi/v1';

/* ============================================================
   STRUCTURE SETTINGS
============================================================ */

const STRUCTURE = {
  '15m': {
    interval: '15m',
    weight: 1,
    candles: 300,
    left: 3,
    right: 3,
    eq: 0.0015
  },

  '1h': {
    interval: '1h',
    weight: 2,
    candles: 300,
    left: 4,
    right: 4,
    eq: 0.0015
  },

  '4h': {
    interval: '4h',
    weight: 3,
    candles: 250,
    left: 5,
    right: 5,
    eq: 0.0020
  },

  '1d': {
    interval: '1d',
    weight: 4,
    candles: 180,
    left: 5,
    right: 5,
    eq: 0.0025
  }
};

/* ============================================================
   LIQUIDITY SETTINGS
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

  orderGapUsd: 35,

  orderMinUsd: 500000,

  sweepMs: 15 * 60 * 1000
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

/* ============================================================
   STATE
============================================================ */

const state = {
  currentPrice: null,

  priceSource: null,

  depthConnected: false,

  snapshotConnected: false,

  klinesConnected: false,

  bookInitialized: false,

  lastUpdateId: 0,

  pendingEvents: [],

  gapDetected: false,

  gapRecovery: false,

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

  lastDepthMessage: null,

  lastKlineMessage: null,

  sweeps: [],

  candleStats: Object.fromEntries(
    Object.keys(STRUCTURE).map(tf => [
      tf,
      {
        requests: 0,
        success: 0,
        errors: 0
      }
    ])
  )
};

/* ============================================================
   HELPERS
============================================================ */

function round(value, decimals = 2) {
  return Number(
    Number(value).toFixed(decimals)
  );
}

function pct(price, current) {
  if (!current) return null;

  return (
    ((price - current) / current) *
    100
  );
}

function iso() {
  return new Date().toISOString();
}

/* ============================================================
   HTTP JSON
============================================================ */

async function getJson(url) {
  const response = await fetch(url, {
    headers: {
      'User-Agent':
        'Binance-Major-Liquidity-Relay/1.0'
    }
  });

  const text =
    await response.text();

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
      `HTTP ${response.status}: ${JSON.stringify(
        data
      ).slice(0, 500)}`
    );
  }

  return data;
}

/* ============================================================
   CANDLE
============================================================ */

function arrayToCandle(row) {
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
   LOAD HISTORICAL CANDLES
============================================================ */

async function loadHistory() {
  for (
    const [tf, cfg] of Object.entries(
      STRUCTURE
    )
  ) {
    state.candleStats[tf].requests += 1;

    try {
      const url =
        `${SPOT_API}/api/v3/klines` +
        `?symbol=${SYMBOL}` +
        `&interval=${cfg.interval}` +
        `&limit=${cfg.candles}`;

      const data =
        await getJson(url);

      candles[tf] =
        Array.isArray(data)
          ? data.map(arrayToCandle)
          : [];

      state.candleStats[tf].success += 1;
    } catch (error) {
      state.candleStats[tf].errors += 1;
    }
  }

  rebuildStructure();
}

/* ============================================================
   PIVOTS
============================================================ */

function pivotHigh(
  data,
  index,
  left,
  right
) {
  if (
    index < left ||
    index + right >= data.length
  ) {
    return false;
  }

  const price =
    data[index].high;

  for (
    let i = index - left;
    i <= index + right;
    i++
  ) {
    if (
      i !== index &&
      data[i].high >= price
    ) {
      return false;
    }
  }

  return true;
}

function pivotLow(
  data,
  index,
  left,
  right
) {
  if (
    index < left ||
    index + right >= data.length
  ) {
    return false;
  }

  const price =
    data[index].low;

  for (
    let i = index - left;
    i <= index + right;
    i++
  ) {
    if (
      i !== index &&
      data[i].low <= price
    ) {
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
  tolerance
) {
  let count = 0;

  for (
    let i = 1;
    i < data.length;
    i++
  ) {
    const candle = data[i];

    const near =
      side === 'BSL'
        ? Math.abs(
            candle.high - price
          ) / price <= tolerance
        : Math.abs(
            candle.low - price
          ) / price <= tolerance;

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
            ...previous.map(
              x => x.high
            )
          )
        : Math.min(
            ...previous.map(
              x => x.low
            )
          );

    const move =
      side === 'BSL'
        ? (base - candle.close) /
          price
        : (candle.close - base) /
          price;

    if (
      move >=
      SETTINGS.reactionPct
    ) {
      count += 1;
    }
  }

  return count;
}

/* ============================================================
   TIMEFRAME STRUCTURE
============================================================ */

function timeframePoints(tf) {
  const cfg =
    STRUCTURE[tf];

  const data =
    candles[tf];

  const highs = [];
  const lows = [];

  for (
    let i = cfg.left;
    i <
      data.length -
        cfg.right;
    i++
  ) {
    if (
      pivotHigh(
        data,
        i,
        cfg.left,
        cfg.right
      )
    ) {
      highs.push({
        price: data[i].high,
        time: data[i].openTime,
        tf
      });
    }

    if (
      pivotLow(
        data,
        i,
        cfg.left,
        cfg.right
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
   EQUAL LEVELS
============================================================ */

function equalLevels(
  points,
  tolerance,
  side
) {
  const output = [];

  for (
    let i = 0;
    i < points.length;
    i++
  ) {
    const cluster = [
      points[i]
    ];

    for (
      let j = i + 1;
      j < points.length;
      j++
    ) {
      const average =
        cluster.reduce(
          (sum, item) =>
            sum + item.price,
          0
        ) /
        cluster.length;

      if (
        Math.abs(
          points[j].price -
            average
        ) /
          average <=
        tolerance
      ) {
        cluster.push(
          points[j]
        );
      }
    }

    if (
      cluster.length >= 2
    ) {
      output.push({
        side,

        price:
          cluster.reduce(
            (sum, item) =>
              sum + item.price,
            0
          ) /
          cluster.length,

        count:
          cluster.length,

        timeframes: [
          ...new Set(
            cluster.map(
              item => item.tf
            )
          )
        ]
      });
    }
  }

  return output;
}

/* ============================================================
   MERGE STRUCTURAL LEVELS
============================================================ */

function mergePoints(
  points,
  side
) {
  const sorted =
    [...points].sort(
      (a, b) =>
        a.price - b.price
    );

  const groups = [];

  for (const point of sorted) {
    const last =
      groups[
        groups.length - 1
      ];

    if (
      last &&
      Math.abs(
        point.price -
          last.priceHigh
      ) <=
        Math.max(
          SETTINGS.mergeUsd,
          last.price *
            SETTINGS.zonePct
        )
    ) {
      last.points.push(
        point
      );

      last.price =
        last.points.reduce(
          (sum, x) =>
            sum + x.price,
          0
        ) /
        last.points.length;

      last.priceLow =
        Math.min(
          last.priceLow,
          point.price
        );

      last.priceHigh =
        Math.max(
          last.priceHigh,
          point.price
        );
    } else {
      groups.push({
        price: point.price,

        priceLow:
          point.price,

        priceHigh:
          point.price,

        points: [point]
      });
    }
  }

  return groups.map(
    group => {
      const timeframes = [
        ...new Set(
          group.points.map(
            x => x.tf
          )
        )
      ];

      const equalCount =
        Math.max(
          ...group.points.map(
            x =>
              x.equalCount ||
              1
          )
        );

      const reactions =
        Math.max(
          ...group.points.map(
            x =>
              x.reactions ||
              0
          )
        );

      const timeframeScore =
        timeframes.reduce(
          (sum, tf) =>
            sum +
            STRUCTURE[tf]
              .weight,
          0
        );

      const score =
        Math.round(
          timeframeScore * 10 +
          Math.min(
            equalCount,
            30
          ) *
            5 +
          Math.min(
            reactions,
            40
          ) *
            2
        );

      return {
        side,

        price:
          group.price,

        priceLow:
          group.priceLow,

        priceHigh:
          group.priceHigh,

        score,

        timeframes,

        equalCount,

        reactions,

        restingUsd: 0,

        restingLevels: []
      };
    }
  );
}

/* ============================================================
   REBUILD STRUCTURE
============================================================ */

function rebuildStructure() {
  const highs = [];
  const lows = [];

  const equalHighs = [];
  const equalLows = [];

  for (
    const tf of Object.keys(
      STRUCTURE
    )
  ) {
    const points =
      timeframePoints(tf);

    highs.push(
      ...points.highs
    );

    lows.push(
      ...points.lows
    );

    equalHighs.push(
      ...equalLevels(
        points.highs,
        STRUCTURE[tf].eq,
        'BSL'
      )
    );

    equalLows.push(
      ...equalLevels(
        points.lows,
        STRUCTURE[tf].eq,
        'SSL'
      )
    );
  }

  const highMetrics =
    highs.map(point => ({
      ...point,

      equalCount:
        Math.max(
          1,
          ...equalHighs
            .filter(
              x =>
                Math.abs(
                  x.price -
                    point.price
                ) /
                  point.price <=
                0.003
            )
            .map(
              x => x.count
            )
        ),

      reactions:
        reactionCount(
          candles[point.tf],
          point.price,
          'BSL',
          STRUCTURE[
            point.tf
          ].eq
        )
    }));

  const lowMetrics =
    lows.map(point => ({
      ...point,

      equalCount:
        Math.max(
          1,
          ...equalLows
            .filter(
              x =>
                Math.abs(
                  x.price -
                    point.price
                ) /
                  point.price <=
                0.003
            )
            .map(
              x => x.count
            )
        ),

      reactions:
        reactionCount(
          candles[point.tf],
          point.price,
          'SSL',
          STRUCTURE[
            point.tf
          ].eq
        )
    }));

  structure.swingHighs =
    highMetrics;

  structure.swingLows =
    lowMetrics;

  structure.equalHighs =
    equalHighs;

  structure.equalLows =
    equalLows;

  structure.levels = [
    ...mergePoints(
      highMetrics,
      'BSL'
    ),

    ...mergePoints(
      lowMetrics,
      'SSL'
    )
  ];

  applyResting();
}

/* ============================================================
   ORDER BOOK ROWS
============================================================ */

function bookRows(side) {
  const map =
    side === 'BSL'
      ? book.asks
      : book.bids;

  const rows = [];

  for (
    const [
      price,
      quantity
    ] of map.entries()
  ) {
    const usd =
      Number(price) *
      Number(quantity);

    if (
      usd >=
      SETTINGS.rawMinUsd
    ) {
      rows.push({
        price: Number(price),

        quantity:
          Number(quantity),

        usd
      });
    }
  }

  rows.sort(
    (a, b) =>
      side === 'BSL'
        ? a.price - b.price
        : b.price - a.price
  );

  return rows;
}

/* ============================================================
   RESTING LIQUIDITY
============================================================ */

function restingNear(
  price,
  side
) {
  const map =
    side === 'BSL'
      ? book.asks
      : book.bids;

  let total = 0;

  const levels = [];

  for (
    const [
      priceKey,
      quantity
    ] of map.entries()
  ) {
    const p =
      Number(priceKey);

    if (
      Math.abs(
        p - price
      ) <=
      SETTINGS.restingRadiusUsd
    ) {
      const usd =
        p *
        Number(quantity);

      if (
        usd >=
        SETTINGS.rawMinUsd
      ) {
        total += usd;

        levels.push({
          price: p,

          quantity:
            Number(quantity),

          usd
        });
      }
    }
  }

  levels.sort(
    (a, b) =>
      b.usd - a.usd
  );

  return {
    total,

    levels:
      levels.slice(0, 10)
  };
}

/* ============================================================
   APPLY ORDER BOOK CONFIRMATION
============================================================ */

function applyResting() {
  for (
    const level of
      structure.levels
  ) {
    const resting =
      restingNear(
        level.price,
        level.side
      );

    level.restingUsd =
      resting.total;

    level.restingLevels =
      resting.levels;

    level.distancePct =
      state.currentPrice
        ? pct(
            level.price,
            state.currentPrice
          )
        : null;

    if (
      resting.total >=
      SETTINGS.restingConfirmUsd
    ) {
      level.score += Math.min(
        25,
        Math.floor(
          resting.total /
            250000
        ) * 5
      );
    }
  }
}

/* ============================================================
   STRENGTH
============================================================ */

function strength(level) {
  if (
    level.score >= 125 ||
    (
      level.timeframes.length >=
        3 &&
      level.equalCount >= 4
    )
  ) {
    return 'HIGH';
  }

  if (
    level.score >= 90 ||
    level.timeframes.length >=
      2 ||
    level.equalCount >= 3
  ) {
    return 'MEDIUM';
  }

  return 'LOW';
}

/* ============================================================
   DECORATE
============================================================ */

function decorate(level) {
  return {
    side: level.side,

    price: round(
      level.price
    ),

    priceLow: round(
      level.priceLow
    ),

    priceHigh: round(
      level.priceHigh
    ),

    score:
      level.score,

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
        level.restingUsd
      ),

    restingConfirmed:
      level.restingUsd >=
      SETTINGS.restingConfirmUsd,

    restingLevels:
      level.restingLevels,

    distancePct:
      state.currentPrice
        ? round(
            pct(
              level.price,
              state.currentPrice
            ),
            3
          )
        : null
  };
}

/* ============================================================
   STRUCTURAL LEVELS
============================================================ */

function structural(side) {
  return structure.levels
    .filter(
      level =>
        level.side === side &&
        level.score >=
          SETTINGS.minScore
    )
    .map(decorate);
}

/* ============================================================
   ORDER BOOK CLUSTERS
============================================================ */

function bookClusters() {
  function cluster(
    rows,
    side
  ) {
    const output = [];

    for (
      const row of rows
    ) {
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
          SETTINGS.orderGapUsd
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

        last.usd +=
          row.usd;

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
          SETTINGS.orderMinUsd
      )
      .map(x => ({
        ...x,

        midpoint:
          round(
            (
              x.priceLow +
              x.priceHigh
            ) / 2
          ),

        usd:
          round(
            x.usd
          )
      }));
  }

  return {
    bids: cluster(
      bookRows('SSL'),
      'bid'
    ),

    asks: cluster(
      bookRows('BSL'),
      'ask'
    )
  };
}

/* ============================================================
   LIVE PRICE
   IMPORTANT FIX
============================================================ */

function updatePrice(
  price,
  source
) {
  const p =
    Number(price);

  if (
    !Number.isFinite(p) ||
    p <= 0
  ) {
    return;
  }

  const previous =
    state.currentPrice;

  state.currentPrice =
    p;

  state.priceSource =
    source ||
    state.priceSource;

  if (previous) {
    detectSweep(
      previous,
      p
    );
  }

  applyResting();

  state.updatedAt =
    iso();
}

/*
   NEW:
   Always derive a live BTC price
   from the best Futures bid/ask.
*/

function updatePriceFromBook() {
  if (
    !book.bids.size &&
    !book.asks.size
  ) {
    return;
  }

  let bestBid =
    -Infinity;

  let bestAsk =
    Infinity;

  for (
    const price of
      book.bids.keys()
  ) {
    if (
      Number(price) >
      bestBid
    ) {
      bestBid =
        Number(price);
    }
  }

  for (
    const price of
      book.asks.keys()
  ) {
    if (
      Number(price) <
      bestAsk
    ) {
      bestAsk =
        Number(price);
    }
  }

  let price =
    null;

  if (
    Number.isFinite(
      bestBid
    ) &&
    Number.isFinite(
      bestAsk
    )
  ) {
    price =
      (
        bestBid +
        bestAsk
      ) / 2;
  } else if (
    Number.isFinite(
      bestBid
    )
  ) {
    price =
      bestBid;
  } else if (
    Number.isFinite(
      bestAsk
    )
  ) {
    price =
      bestAsk;
  }

  if (price) {
    updatePrice(
      price,
      'futures_orderbook_mid'
    );
  }
}

/* ============================================================
   SWEEP DETECTION
============================================================ */

function recordSweep(
  side,
  level,
  price
) {
  const last =
    state.sweeps[
      state.sweeps.length - 1
    ];

  if (
    last &&
    last.side === side &&
    Math.abs(
      last.level - level
    ) < 20 &&
    Date.now() -
      new Date(
        last.time
      ).getTime() <
      SETTINGS.sweepMs
  ) {
    return;
  }

  state.sweeps.push({
    side,

    level:
      round(level),

    price:
      round(price),

    time:
      iso()
  });

  if (
    state.sweeps.length >
    30
  ) {
    state.sweeps.shift();
  }
}

function detectSweep(
  previous,
  current
) {
  for (
    const level of
      structure.levels
  ) {
    if (
      level.side === 'BSL' &&
      previous <
        level.price &&
      current >=
        level.price
    ) {
      recordSweep(
        'BSL',
        level.price,
        current
      );
    }

    if (
      level.side === 'SSL' &&
      previous >
        level.price &&
      current <=
        level.price
    ) {
      recordSweep(
        'SSL',
        level.price,
        current
      );
    }
  }
}

/* ============================================================
   INTELLIGENCE
============================================================ */

function intelligence() {
  applyResting();

  const current =
    state.currentPrice;

  if (!current) {
    return {
      state:
        'WAITING FOR LIVE PRICE',

      nearestBSL:
        null,

      nearestSSL:
        null,

      nextBSL: [],

      nextSSL: [],

      orderBook:
        bookClusters(),

      recentSweeps:
        state.sweeps
          .slice(-10)
          .reverse()
    };
  }

  const bsl =
    structural('BSL')
      .filter(
        level =>
          level.price >
          current
      )
      .sort(
        (a, b) =>
          a.price - b.price
      );

  const ssl =
    structural('SSL')
      .filter(
        level =>
          level.price <
          current
      )
      .sort(
        (a, b) =>
          b.price - a.price
      );

  return {
    state:
      bsl[0] && ssl[0]
        ? 'BETWEEN LIQUIDITY'
        : bsl[0]
        ? 'UPSIDE LIQUIDITY ONLY'
        : ssl[0]
        ? 'DOWNSIDE LIQUIDITY ONLY'
        : 'NO STRUCTURAL LIQUIDITY',

    nearestBSL:
      bsl[0] || null,

    nearestSSL:
      ssl[0] || null,

    nextBSL:
      bsl.slice(0, 5),

    nextSSL:
      ssl.slice(0, 5),

    orderBook:
      bookClusters(),

    recentSweeps:
      state.sweeps
        .slice(-10)
        .reverse()
  };
}

/* ============================================================
   PUBLIC STRUCTURE
============================================================ */

function publicStructure() {
  const bsl =
    structural('BSL')
      .sort(
        (a, b) =>
          Math.abs(
            a.price -
              (
                state.currentPrice ||
                0
              )
          ) -
          Math.abs(
            b.price -
              (
                state.currentPrice ||
                0
              )
          )
      )
      .slice(
        0,
        SETTINGS.maxLevels
      );

  const ssl =
    structural('SSL')
      .sort(
        (a, b) =>
          Math.abs(
            a.price -
              (
                state.currentPrice ||
                0
              )
          ) -
          Math.abs(
            b.price -
              (
                state.currentPrice ||
                0
              )
          )
      )
      .slice(
        0,
        SETTINGS.maxLevels
      );

  const resistance =
    bsl
      .filter(
        level =>
          state.currentPrice
            ? level.price >
              state.currentPrice
            : true
      )
      .sort(
        (a, b) =>
          a.price - b.price
      )
      .slice(0, 3);

  const support =
    ssl
      .filter(
        level =>
          state.currentPrice
            ? level.price <
              state.currentPrice
            : true
      )
      .sort(
        (a, b) =>
          b.price - a.price
      )
      .slice(0, 3);

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
      resistance.slice(
        0,
        1
      ),

    nearestSSL:
      support.slice(
        0,
        1
      )
  };
}

/* ============================================================
   RAW LIQUIDITY
============================================================ */

function rawLiquidity() {
  return {
    largeBids:
      bookRows('SSL')
        .sort(
          (a, b) =>
            b.usd - a.usd
        )
        .slice(0, 25),

    largeAsks:
      bookRows('BSL')
        .sort(
          (a, b) =>
            b.usd - a.usd
        )
        .slice(0, 25)
  };
}

/* ============================================================
   APPLY DEPTH EVENT
============================================================ */

function applyDepth(message) {
  for (
    const [
      price,
      quantity
    ] of message.b || []
  ) {
    const p =
      Number(price);

    const q =
      Number(quantity);

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
    ] of message.a || []
  ) {
    const p =
      Number(price);

    const q =
      Number(quantity);

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
      message.u ||
        state.lastUpdateId
    );

  state.lastDepthMessage =
    iso();

  /*
     IMPORTANT:
     Every depth update now refreshes
     the live BTC price.
  */

  updatePriceFromBook();
}

/* ============================================================
   SNAPSHOT
============================================================ */

let snapshotBusy =
  false;

let lastSnapshotAt =
  0;

function requestSnapshot() {
  if (
    snapshotBusy ||
    Date.now() -
      lastSnapshotAt <
      65000
  ) {
    return;
  }

  snapshotBusy =
    true;

  lastSnapshotAt =
    Date.now();

  state.snapshotRequests +=
    1;

  let ws;

  try {
    ws =
      new WebSocket(
        SNAPSHOT_WS
      );

    const id =
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
            id,

            method:
              'depth',

            params: {
              symbol:
                SYMBOL,

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
            data.id !== id
          ) {
            return;
          }

          if (
            data.status &&
            data.status !== 200
          ) {
            state.snapshotLastStatus =
              data.status;

            state.snapshotLastError =
              JSON.stringify(
                data.error ||
                  data
              );

            return;
          }

          if (
            !data.result
          ) {
            return;
          }

          clearTimeout(
            timer
          );

          book.bids.clear();
          book.asks.clear();

          for (
            const [
              price,
              quantity
            ] of
              data.result
                .bids || []
          ) {
            if (
              Number(quantity) >
              0
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
            ] of
              data.result
                .asks || []
          ) {
            if (
              Number(quantity) >
              0
            ) {
              book.asks.set(
                Number(price),
                Number(quantity)
              );
            }
          }

          state.lastUpdateId =
            Number(
              data.result
                .lastUpdateId
            );

          state.snapshotId =
            state.lastUpdateId;

          state.bookInitialized =
            true;

          state.gapRecovery =
            false;

          state.gapDetected =
            false;

          state.snapshotLastStatus =
            200;

          state.snapshotLastError =
            null;

          state.lastSnapshotResponse =
            iso();

          /*
             Re-apply buffered
             depth events.
          */

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
            const event of
              pending
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
              applyDepth(
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
              applyDepth(
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

          /*
             CRITICAL:
             Set price from snapshot
             immediately.
          */

          updatePriceFromBook();

          applyResting();

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
  }

  setTimeout(() => {
    snapshotBusy =
      false;
  }, 1000);
}

/* ============================================================
   DEPTH WEBSOCKET
============================================================ */

function connectDepth() {
  const ws =
    new WebSocket(
      DEPTH_WS
    );

  state.depthConnected =
    false;

  ws.on(
    'open',
    () => {
      state.depthConnected =
        true;

      requestSnapshot();
    }
  );

  ws.on(
    'message',
    raw => {
      try {
        const message =
          JSON.parse(
            raw.toString()
          );

        if (
          message.e !==
          'depthUpdate'
        ) {
          return;
        }

        const U =
          Number(
            message.U
          );

        const u =
          Number(
            message.u
          );

        /*
           Before snapshot:
           buffer events.
        */

        if (
          !state.bookInitialized
        ) {
          if (
            state.pendingEvents
              .length < 5000
          ) {
            state.pendingEvents.push(
              message
            );
          }

          return;
        }

        /*
           Already processed.
        */

        if (
          u <=
          state.lastUpdateId
        ) {
          return;
        }

        /*
           Real sequence gap.
        */

        if (
          U >
          state.lastUpdateId +
            1
        ) {
          state.gapDetected =
            true;

          state.gapRecovery =
            true;

          state.gapCount +=
            1;

          state.lastGapLocal =
            state.lastUpdateId;

          state.lastGapIncoming =
            U;

          state.bookInitialized =
            false;

          state.pendingEvents = [
            message
          ];

          requestSnapshot();

          return;
        }

        applyDepth(
          message
        );
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
   KLINE WEBSOCKET
============================================================ */

function connectKlines() {
  const ws =
    new WebSocket(
      KLINE_WS
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

        if (
          !candles[tf]
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
            Number(k.T)
        };

        const data =
          candles[tf];

        const index =
          data.findIndex(
            x =>
              x.openTime ===
              candle.openTime
          );

        if (
          index >= 0
        ) {
          data[index] =
            candle;
        } else {
          data.push(
            candle
          );

          if (
            data.length >
            STRUCTURE[tf]
              .candles
          ) {
            data.shift();
          }
        }

        /*
           Kline is also a
           live price source.
        */

        updatePrice(
          candle.close,
          'futures_kline'
        );

        state.lastKlineMessage =
          iso();

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
   HEALTH
============================================================ */

function health() {
  return {
    ok: true,

    service:
      'Binance BTCUSDT Major Liquidity Relay',

    symbol:
      SYMBOL,

    currentPrice:
      state.currentPrice,

    priceSource:
      state.priceSource,

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
          ([tf, data]) => [
            tf,
            data.length
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

    structure: {
      swingHighs:
        structure
          .swingHighs.length,

      swingLows:
        structure
          .swingLows.length,

      equalHighs:
        structure
          .equalHighs.length,

      equalLows:
        structure
          .equalLows.length,

      majorBSL:
        structural(
          'BSL'
        ).length,

      majorSSL:
        structural(
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
   API
============================================================ */

app.get(
  '/',
  (req, res) => {
    res.json(
      health()
    );
  }
);

app.get(
  '/health',
  (req, res) => {
    res.json(
      health()
    );
  }
);

app.get(
  '/intelligence',
  (req, res) => {
    res.json({
      ok: true,

      symbol:
        SYMBOL,

      currentPrice:
        state.currentPrice,

      priceSource:
        state.priceSource,

      intelligence:
        intelligence(),

      updatedAt:
        state.updatedAt
    });
  }
);

app.get(
  '/structure',
  (req, res) => {
    res.json({
      ok: true,

      symbol:
        SYMBOL,

      currentPrice:
        state.currentPrice,

      priceSource:
        state.priceSource,

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
              ([tf, data]) => [
                tf,
                data.length
              ]
            )
          ),

        swingHighs:
          structure
            .swingHighs.length,

        swingLows:
          structure
            .swingLows.length,

        equalHighs:
          structure
            .equalHighs.length,

        equalLows:
          structure
            .equalLows.length
      },

      updatedAt:
        state.updatedAt
    });
  }
);

app.get(
  '/liquidity',
  (req, res) => {
    res.json({
      ok: true,

      symbol:
        SYMBOL,

      currentPrice:
        state.currentPrice,

      priceSource:
        state.priceSource,

      marketStructure:
        publicStructure(),

      intelligence:
        intelligence(),

      rawRestingLiquidity:
        rawLiquidity(),

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
              ([tf, data]) => [
                tf,
                data.length
              ]
            )
          ),

        swingHighs:
          structure
            .swingHighs.length,

        swingLows:
          structure
            .swingLows.length,

        equalHighs:
          structure
            .equalHighs.length,

        equalLows:
          structure
            .equalLows.length
      },

      updatedAt:
        state.updatedAt
    });
  }
);

app.get(
  '/book',
  (req, res) => {
    res.json({
      ok: true,

      symbol:
        SYMBOL,

      currentPrice:
        state.currentPrice,

      priceSource:
        state.priceSource,

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
      /*
         Keep price alive even if
         no kline message arrives.
      */

      updatePriceFromBook();

      applyResting();
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
        data =>
          data.length > 0
      );

    if (ready) {
      rebuildStructure();
    }
  },
  60000
);

/* ============================================================
   START
============================================================ */

(async () => {
  await loadHistory();

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
})().catch(
  error => {
    console.error(
      'STARTUP ERROR',
      error
    );

    process.exit(1);
  }
);
