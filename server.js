const express = require("express");
const WebSocket = require("ws");

const app = express();

const PORT = process.env.PORT || 10000;

const SYMBOL = "BTCUSDT";
const SYMBOL_LOWER = SYMBOL.toLowerCase();

const FUTURES_REST =
  "https://fapi.binance.com";

const FUTURES_WS =
  `wss://fstream.binance.com/ws/${SYMBOL_LOWER}@depth20@100ms`;

// ============================================================
// SETTINGS
// ============================================================

// Wider REST order-book snapshot
const ORDERBOOK_LIMIT = 1000;

// Refresh the wide order book every 5 seconds
const REST_REFRESH_MS = 5000;

// Only consider liquidity within this distance from price
const DEFAULT_RANGE_PCT = 0.03;

// Minimum total USD value for a cluster
const DEFAULT_MIN_CLUSTER_USD = 500000;

// Neighboring orders within this percentage are grouped
const DEFAULT_CLUSTER_GAP_PCT = 0.0002;

// Maximum number of clusters returned per side
const MAX_CLUSTERS_PER_SIDE = 10;

// Persistence tracking
const PERSISTENCE_MATCH_PCT = 0.0015;

// ============================================================
// STATE
// ============================================================

let latestBook = {
  symbol: SYMBOL,
  currentPrice: null,

  bids: [],
  asks: [],

  bidClusters: [],
  askClusters: [],

  updatedAt: null,
  restUpdatedAt: null,

  websocketConnected: false,
  restConnected: false,

  depthLevels: 0,

  status: "starting"
};

let ws = null;
let reconnectTimer = null;
let restTimer = null;

let previousClusters = {
  bids: [],
  asks: []
};

// ============================================================
// HELPERS
// ============================================================

function number(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
}

function round(value, decimals = 2) {
  const factor = Math.pow(10, decimals);
  return Math.round(value * factor) / factor;
}

function formatUsd(value) {
  if (value >= 1000000000) {
    return `$${(value / 1000000000).toFixed(2)}B`;
  }

  if (value >= 1000000) {
    return `$${(value / 1000000).toFixed(2)}M`;
  }

  if (value >= 1000) {
    return `$${(value / 1000).toFixed(1)}K`;
  }

  return `$${value.toFixed(0)}`;
}

function distancePct(price, currentPrice) {
  if (!currentPrice) return 0;

  return ((price - currentPrice) / currentPrice) * 100;
}

function getCurrentPrice() {
  if (
    latestBook.bids.length > 0 &&
    latestBook.asks.length > 0
  ) {
    const bestBid = latestBook.bids[0].price;
    const bestAsk = latestBook.asks[0].price;

    return (bestBid + bestAsk) / 2;
  }

  if (latestBook.bids.length > 0) {
    return latestBook.bids[0].price;
  }

  if (latestBook.asks.length > 0) {
    return latestBook.asks[0].price;
  }

  return null;
}

// ============================================================
// BINANCE WEBSOCKET
// ============================================================

function connectBinanceWebSocket() {
  console.log("Connecting to Binance Futures WebSocket...");

  try {
    ws = new WebSocket(FUTURES_WS);

    ws.on("open", () => {
      console.log(
        "Connected to Binance Futures WebSocket."
      );

      latestBook.websocketConnected = true;
    });

    ws.on("message", (data) => {
      try {
        const message = JSON.parse(data.toString());

        if (!message.b || !message.a) {
          return;
        }

        const bids = message.b.map((level) => {
          const price = number(level[0]);
          const quantity = number(level[1]);

          return {
            price,
            quantity,
            usd: price * quantity
          };
        });

        const asks = message.a.map((level) => {
          const price = number(level[0]);
          const quantity = number(level[1]);

          return {
            price,
            quantity,
            usd: price * quantity
          };
        });

        latestBook.bids = bids;
        latestBook.asks = asks;

        latestBook.currentPrice = getCurrentPrice();

        latestBook.updatedAt =
          new Date().toISOString();

        latestBook.websocketConnected = true;
      } catch (error) {
        console.log(
          "WebSocket message parse error:",
          error.message
        );
      }
    });

    ws.on("close", () => {
      console.log(
        "Binance Futures WebSocket disconnected."
      );

      latestBook.websocketConnected = false;

      scheduleWebSocketReconnect();
    });

    ws.on("error", (error) => {
      console.log(
        "Binance Futures WebSocket error:",
        error.message
      );
    });
  } catch (error) {
    console.log(
      "WebSocket connection error:",
      error.message
    );

    scheduleWebSocketReconnect();
  }
}

function scheduleWebSocketReconnect() {
  if (reconnectTimer) {
    return;
  }

  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;

    connectBinanceWebSocket();
  }, 5000);
}

// ============================================================
// WIDE REST ORDER BOOK
// ============================================================

async function refreshWideOrderBook() {
  try {
    const url =
      `${FUTURES_REST}/fapi/v1/depth` +
      `?symbol=${SYMBOL}` +
      `&limit=${ORDERBOOK_LIMIT}`;

    const response = await fetch(url);

    if (!response.ok) {
      throw new Error(
        `HTTP ${response.status}`
      );
    }

    const data = await response.json();

    const bids = Array.isArray(data.bids)
      ? data.bids.map((level) => {
          const price = number(level[0]);
          const quantity = number(level[1]);

          return {
            price,
            quantity,
            usd: price * quantity
          };
        })
      : [];

    const asks = Array.isArray(data.asks)
      ? data.asks.map((level) => {
          const price = number(level[0]);
          const quantity = number(level[1]);

          return {
            price,
            quantity,
            usd: price * quantity
          };
        })
      : [];

    bids.sort((a, b) => b.price - a.price);
    asks.sort((a, b) => a.price - b.price);

    latestBook.bids = bids;
    latestBook.asks = asks;

    latestBook.currentPrice =
      getCurrentPrice();

    latestBook.restUpdatedAt =
      new Date().toISOString();

    latestBook.restConnected = true;

    latestBook.depthLevels =
      bids.length + asks.length;

    latestBook.status = "live";

    updateLiquidityClusters();

    console.log(
      `Wide order book updated: ${bids.length} bids / ${asks.length} asks`
    );
  } catch (error) {
    latestBook.restConnected = false;

    console.log(
      "Wide order book error:",
      error.message
    );
  }
}

function startRestRefresh() {
  if (restTimer) {
    clearInterval(restTimer);
  }

  refreshWideOrderBook();

  restTimer = setInterval(() => {
    refreshWideOrderBook();
  }, REST_REFRESH_MS);
}

// ============================================================
// CLUSTER ENGINE
// ============================================================

function buildClusters(
  levels,
  side,
  currentPrice,
  minClusterUsd,
  rangePct,
  gapPct
) {
  if (!currentPrice) {
    return [];
  }

  if (!levels || levels.length === 0) {
    return [];
  }

  const filtered = [];

  for (const level of levels) {
    if (!level || level.price <= 0) {
      continue;
    }

    const distance =
      Math.abs(
        (level.price - currentPrice) /
          currentPrice
      );

    if (distance > rangePct) {
      continue;
    }

    // Buy liquidity should be below current price.
    if (
      side === "bid" &&
      level.price >= currentPrice
    ) {
      continue;
    }

    // Sell liquidity should be above current price.
    if (
      side === "ask" &&
      level.price <= currentPrice
    ) {
      continue;
    }

    filtered.push(level);
  }

  if (filtered.length === 0) {
    return [];
  }

  if (side === "bid") {
    filtered.sort(
      (a, b) => b.price - a.price
    );
  } else {
    filtered.sort(
      (a, b) => a.price - b.price
    );
  }

  const clusters = [];

  let cluster = null;

  for (const level of filtered) {
    if (!cluster) {
      cluster = createCluster(
        level,
        side
      );

      continue;
    }

    const previousPrice =
      cluster.lastPrice;

    const priceGap =
      Math.abs(
        level.price - previousPrice
      );

    const allowedGap =
      currentPrice * gapPct;

    if (priceGap <= allowedGap) {
      addLevelToCluster(
        cluster,
        level
      );
    } else {
      clusters.push(cluster);

      cluster = createCluster(
        level,
        side
      );
    }
  }

  if (cluster) {
    clusters.push(cluster);
  }

  // Only retain meaningful clusters
  const strongClusters =
    clusters.filter(
      (cluster) =>
        cluster.totalUsd >=
        minClusterUsd
    );

  // Sort by total USD value
  strongClusters.sort(
    (a, b) =>
      b.totalUsd - a.totalUsd
  );

  return strongClusters
    .slice(0, MAX_CLUSTERS_PER_SIDE)
    .map((cluster) => {
      return finalizeCluster(
        cluster,
        currentPrice
      );
    });
}

function createCluster(level, side) {
  return {
    side,

    firstPrice: level.price,
    lastPrice: level.price,

    minPrice: level.price,
    maxPrice: level.price,

    totalUsd: level.usd,
    totalQuantity: level.quantity,

    levels: 1,

    strongestPrice: level.price,
    strongestUsd: level.usd
  };
}

function addLevelToCluster(
  cluster,
  level
) {
  cluster.lastPrice =
    level.price;

  cluster.minPrice =
    Math.min(
      cluster.minPrice,
      level.price
    );

  cluster.maxPrice =
    Math.max(
      cluster.maxPrice,
      level.price
    );

  cluster.totalUsd +=
    level.usd;

  cluster.totalQuantity +=
    level.quantity;

  cluster.levels++;

  if (
    level.usd >
    cluster.strongestUsd
  ) {
    cluster.strongestUsd =
      level.usd;

    cluster.strongestPrice =
      level.price;
  }
}

function finalizeCluster(
  cluster,
  currentPrice
) {
  const midpoint =
    (cluster.minPrice +
      cluster.maxPrice) /
    2;

  const distance =
    distancePct(
      midpoint,
      currentPrice
    );

  return {
    side: cluster.side,

    priceLow: round(
      cluster.minPrice,
      2
    ),

    priceHigh: round(
      cluster.maxPrice,
      2
    ),

    midpoint: round(
      midpoint,
      2
    ),

    strongestPrice: round(
      cluster.strongestPrice,
      2
    ),

    strongestUsd:
      round(
        cluster.strongestUsd,
        2
      ),

    totalUsd:
      round(
        cluster.totalUsd,
        2
      ),

    totalQuantity:
      round(
        cluster.totalQuantity,
        6
      ),

    levels:
      cluster.levels,

    distancePct:
      round(
        distance,
        3
      ),

    persistence:
      1
  };
}

// ============================================================
// PERSISTENCE
// ============================================================

function updatePersistence(
  newClusters,
  oldClusters
) {
  return newClusters.map(
    (cluster) => {
      let bestMatch = null;

      let bestDistance =
        Infinity;

      for (const oldCluster of oldClusters) {
        if (
          oldCluster.side !==
          cluster.side
        ) {
          continue;
        }

        const distance =
          Math.abs(
            oldCluster.midpoint -
              cluster.midpoint
          ) /
          cluster.midpoint;

        if (
          distance <
            PERSISTENCE_MATCH_PCT &&
          distance <
            bestDistance
        ) {
          bestMatch =
            oldCluster;

          bestDistance =
            distance;
        }
      }

      if (bestMatch) {
        cluster.persistence =
          (bestMatch.persistence ||
            1) + 1;
      }

      return cluster;
    }
  );
}

// ============================================================
// UPDATE CLUSTERS
// ============================================================

function updateLiquidityClusters() {
  const currentPrice =
    latestBook.currentPrice;

  if (!currentPrice) {
    return;
  }

  const rangePct =
    DEFAULT_RANGE_PCT;

  const minClusterUsd =
    DEFAULT_MIN_CLUSTER_USD;

  const gapPct =
    DEFAULT_CLUSTER_GAP_PCT;

  let bidClusters =
    buildClusters(
      latestBook.bids,
      "bid",
      currentPrice,
      minClusterUsd,
      rangePct,
      gapPct
    );

  let askClusters =
    buildClusters(
      latestBook.asks,
      "ask",
      currentPrice,
      minClusterUsd,
      rangePct,
      gapPct
    );

  bidClusters =
    updatePersistence(
      bidClusters,
      previousClusters.bids
    );

  askClusters =
    updatePersistence(
      askClusters,
      previousClusters.asks
    );

  latestBook.bidClusters =
    bidClusters;

  latestBook.askClusters =
    askClusters;

  previousClusters = {
    bids: bidClusters.map(
      (cluster) => ({
        ...cluster
      })
    ),

    asks: askClusters.map(
      (cluster) => ({
        ...cluster
      })
    )
  };
}

// ============================================================
// API
// ============================================================

app.get("/", (req, res) => {
  res.json({
    service:
      "Binance Futures Liquidity Relay",

    symbol: SYMBOL,

    status:
      latestBook.status,

    binanceWebSocket:
      latestBook.websocketConnected,

    binanceRest:
      latestBook.restConnected,

    currentPrice:
      latestBook.currentPrice,

    depthLevels:
      latestBook.depthLevels,

    bidClusters:
      latestBook.bidClusters.length,

    askClusters:
      latestBook.askClusters.length,

    updatedAt:
      latestBook.updatedAt,

    restUpdatedAt:
      latestBook.restUpdatedAt
  });
});

app.get("/health", (req, res) => {
  res.json({
    ok: true,

    symbol: SYMBOL,

    status:
      latestBook.status,

    websocketConnected:
      latestBook.websocketConnected,

    restConnected:
      latestBook.restConnected,

    currentPrice:
      latestBook.currentPrice,

    depthLevels:
      latestBook.depthLevels,

    updatedAt:
      latestBook.updatedAt,

    restUpdatedAt:
      latestBook.restUpdatedAt
  });
});

// ============================================================
// RAW WIDE ORDER BOOK
// ============================================================

app.get("/book", (req, res) => {
  res.json({
    symbol: SYMBOL,

    currentPrice:
      latestBook.currentPrice,

    bids:
      latestBook.bids,

    asks:
      latestBook.asks,

    bidCount:
      latestBook.bids.length,

    askCount:
      latestBook.asks.length,

    updatedAt:
      latestBook.updatedAt,

    restUpdatedAt:
      latestBook.restUpdatedAt,

    websocketConnected:
      latestBook.websocketConnected,

    restConnected:
      latestBook.restConnected
  });
});

// ============================================================
// LIQUIDITY CLUSTERS
// ============================================================

app.get("/liquidity", (req, res) => {
  const requestedMinUsd =
    number(
      req.query.minUsd
    );

  const requestedRangePct =
    number(
      req.query.rangePct
    );

  const requestedGapPct =
    number(
      req.query.gapPct
    );

  const minClusterUsd =
    requestedMinUsd > 0
      ? requestedMinUsd
      : DEFAULT_MIN_CLUSTER_USD;

  const rangePct =
    requestedRangePct > 0
      ? requestedRangePct
      : DEFAULT_RANGE_PCT;

  const gapPct =
    requestedGapPct > 0
      ? requestedGapPct / 100
      : DEFAULT_CLUSTER_GAP_PCT;

  const currentPrice =
    latestBook.currentPrice;

  const bidClusters =
    buildClusters(
      latestBook.bids,
      "bid",
      currentPrice,
      minClusterUsd,
      rangePct,
      gapPct
    );

  const askClusters =
    buildClusters(
      latestBook.asks,
      "ask",
      currentPrice,
      minClusterUsd,
      rangePct,
      gapPct
    );

  const persistedBids =
    updatePersistence(
      bidClusters,
      previousClusters.bids
    );

  const persistedAsks =
    updatePersistence(
      askClusters,
      previousClusters.asks
    );

  res.json({
    symbol: SYMBOL,

    currentPrice:

      currentPrice
        ? round(
            currentPrice,
            2
          )
        : null,

    settings: {
      minClusterUsd,
      rangePct,
      gapPct
    },

    buyLiquidity:
      persistedBids,

    sellLiquidity:
      persistedAsks,

    summary: {
      buyClusters:
        persistedBids.length,

      sellClusters:
        persistedAsks.length,

      totalBuyUsd:
        round(
          persistedBids.reduce(
            (sum, cluster) =>
              sum +
              cluster.totalUsd,
            0
          ),
          2
        ),

      totalSellUsd:
        round(
          persistedAsks.reduce(
            (sum, cluster) =>
              sum +
              cluster.totalUsd,
            0
          ),
          2
        )
    },

    restConnected:
      latestBook.restConnected,

    websocketConnected:
      latestBook.websocketConnected,

    depthLevels:
      latestBook.depthLevels,

    updatedAt:
      latestBook.restUpdatedAt
  });
});

// ============================================================
// START SERVER
// ============================================================

app.listen(PORT, () => {
  console.log(
    `Relay server running on port ${PORT}`
  );

  connectBinanceWebSocket();

  startRestRefresh();
});
