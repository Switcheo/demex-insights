'use strict'

const { getMarketPrices } = require('./prices');

// getOpenPositions returns the currently open positions for the given address along with the position's underlying token denom
async function getOpenPositions(client, address) {
  const query = `
    SELECT
      positions.entry_price * (10 ^ (markets.base_precision - markets.quote_precision)) AS entry_price,
      positions.lots * (10 ^ -markets.base_precision)::decimal AS lots,
      markets.id AS market_id,
      markets.base_precision - markets.quote_precision AS price_decimals
    FROM open_positions
    INNER JOIN positions ON positions.id = open_positions.id
    INNER JOIN markets ON markets.id = open_positions.market
    WHERE open_positions.address = $1
    ;
  `

  const { rows: positions } = await client.query(query, [address])
  return positions
}

// getOpenPositionUPnl returns the total unrealized PNL of all open positions for the address
async function getOpenPositionUPnl(client, address) {
  const prices = await getMarketPrices()
  const positions = await getOpenPositions(client, address)

  let upnl = 0.0
  for (const p of positions) {
    const raw_price = prices.get(p['market_id'])

    if (!raw_price) continue

    const mark_price = parseFloat(raw_price) * (10 ** parseInt(p['price_decimals'], 10))

    upnl += parseFloat(p['lots']) * (mark_price - parseFloat(p['entry_price']))
  }

  return upnl
}

const TotalRPNLQuery = `
  WITH h AS (
    SELECT
      f.hour,
      f.address,
      f.market,
      CASE
         WHEN p.closed_block_height = 0 THEN
          -- since we are using snapshots, minus of the previous rpnl which has either been closed and accounted for in 'hourly_closed_rpnl',
          -- or is being carried to this current open position which we should net off from previous snapshot
          p.realized_pnl - lead(p.realized_pnl, 1, 0) OVER (PARTITION BY f.address, f.market ORDER BY f.hour DESC)
        ELSE 0 -- if this is a closed position, it is already fully accounted for in 'hourly_closed_rpnl' below
      END AS rpnl
    FROM hourly_final_position_ids f
    JOIN archived_positions p ON p.id = f.id
    WHERE f.address = $1
    AND f.hour >= $2 AND f.hour <= $3
  ),
  j AS (
    SELECT
      h.hour,
      SUM(COALESCE(c.total_realized_pnl, 0)) + SUM(COALESCE(h.rpnl, 0)) AS rpnl
    FROM h
    LEFT OUTER JOIN hourly_closed_rpnl c ON c.hour = h.hour AND c.address = h.address
    GROUP BY h.hour
  )
  SELECT
    time_bucket('1 day', j.hour) AS day,
    SUM(j.rpnl) * (10 ^ -18)::decimal AS rpnl
  FROM j
  GROUP BY day
  ORDER BY day ASC;
`

module.exports = {
  getOpenPositionUPnl,
  TotalRPNLQuery,
}
