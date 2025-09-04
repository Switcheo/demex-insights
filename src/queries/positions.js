'use strict'

const { withinOneDay } = require('../helpers/time')
const { getMarketPrices } = require('./prices')

// getOpenPositions returns the currently open positions for the given address along with the position's underlying token denom
async function getOpenPositions(client, address, market) {
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
    ${market ? 'AND open_positions.market = $2' : ''}
    ;
  `

  const params = [address]
  if (market) {
    params.push(market)
  }
  const { rows: positions } = await client.query(query, params)
  return positions
}

// getOpenPositionUPnl returns the total unrealized PNL of all open positions for the address
async function getOpenPositionUPnl(client, address, market = null) {
  const prices = await getMarketPrices()
  const positions = await getOpenPositions(client, address, market)

  let upnl = 0.0
  for (const p of positions) {
    const raw_price = prices.get(p['market_id'])

    if (!raw_price) continue

    const mark_price = parseFloat(raw_price) * (10 ** parseInt(p['price_decimals'], 10))

    upnl += parseFloat(p['lots']) * (mark_price - parseFloat(p['entry_price']))
  }

  return upnl
}

// getDailyRPNLQuery is unable to get pnl for specific positions, but is very fast
// use getRPNLQuery to get hourly / daily pnl for specific positions.
function getDailyRPNLQuery({ address, market = null, from, to }) {
  const params = [address, from, to]
  if (market) {
    params.push(market)
  }

  const query = `
    WITH h AS (
      SELECT
        f.hour,
        f.address,
        ${market ? '' : 'f.market,'}
        CASE
          WHEN p.closed_block_height = 0 THEN
            -- since we are using snapshots, minus of the previous rpnl which has either been closed and accounted for in 'hourly_closed_rpnl',
            -- or is being carried to this current open position which we should net off from previous snapshot
            p.realized_pnl - LEAD(p.realized_pnl, 1, 0) OVER (PARTITION BY f.address, f.market ORDER BY f.hour DESC)
          ELSE 0 -- if this is a closed position, it is already fully accounted for in 'hourly_closed_rpnl' below
        END AS rpnl
      FROM hourly_final_position_ids f
      JOIN archived_positions p ON p.id = f.id
      WHERE f.address = $1
      AND f.hour >= $2 AND f.hour <= $3
      ${market ? 'AND f.market = $4' : ''}
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

  return [query, params]
}

// getRPNLQuery gets rpnl delta for an address (and optionally market),
// but can be slow for very large timeframes.
const getRPNLQuery = ({ address, market = null, from, to }) => {
  const bucketInterval = withinOneDay(from, to) ? 'hour' : 'day'

  const params = [address, from, to]
  if (market) {
    params.push(market)
  }

  const query = `
    ${getConstrainedPositionsCTE({ filterSpecificMarket: !!market })}
    SELECT
      time_bucket(INTERVAL '1 ${bucketInterval}', timestamp) AS time,
      SUM(f.rpnl_delta) * (10 ^ -18) as rpnl
    FROM f
    WHERE timestamp >= $2 -- this is repeated because data may contain entries outside of the relevant bucket (but is needed due to usage of LAG above)
    AND time_bucket(INTERVAL '1 ${bucketInterval}', timestamp) >= $2 -- this is needed because block filter is not precise
    AND time_bucket(INTERVAL '1 ${bucketInterval}', timestamp) <= $3 -- as above
    GROUP BY time
    ORDER BY time ASC;
  `

  return [query, params]
}

// getConstrainedPositionsCTE returns the CTE query fragment that quickly selects the relevant archived_positions for
// the given $1=address, $2=from, $3=to.
//
// Note that the final selected positions in result subtable `f` still needs to be time_bucketed and constrained accordingly.
const getConstrainedPositionsCTE = ({ filterSpecificMarket = false } = {}) => {
  return `
    WITH min_b AS (
        SELECT MIN(block_height) AS min_height
        FROM blocks
        WHERE blocks.time >= $2
        AND blocks.time <= $2::TIMESTAMPTZ + INTERVAL '1 day' -- extra constrain to find correct hypertable chunk
      ),
      max_b AS (
        SELECT MAX(block_height) AS max_height
        FROM blocks
        WHERE blocks.time <= $3::TIMESTAMPTZ + INTERVAL '1 day' -- as above, but inversed as the timebucket is aligned left (so we find the max block as "to" -> "to+1day")
        AND blocks.time >= LEAST(NOW(), $3::TIMESTAMPTZ) - INTERVAL '1 hour' -- give some buffer in case user puts in an exact time and the latest block has not occured yet (i.e. "to-1hr" -> "to+1day")
      ),
      p AS (
        SELECT
          *
        FROM archived_positions
        WHERE address = $1
        ${filterSpecificMarket ? 'AND market = $4' : ''}
        AND archived_positions.opened_block_height >= (SELECT min_height FROM min_b)
        AND archived_positions.updated_block_height <= (SELECT max_height FROM max_b)
      ),
      f AS (
        SELECT
          ${filterSpecificMarket ? '' : 'p.market,'}
          p.update_reason,
          p.lots,
          b.time AS timestamp,
          realized_pnl - LAG(realized_pnl, 1, 0) OVER (PARTITION BY p.market, p.opened_block_height ORDER BY p.updated_block_height ASC) AS rpnl_delta
        FROM p
        JOIN blocks b ON b.block_height = p.updated_block_height
        AND b.block_height >= (SELECT min_height FROM min_b) -- constrain hypertable chunks
        AND b.block_height <= (SELECT max_height FROM max_b)
      )
    `
}

module.exports = {
  getOpenPositionUPnl,
  getRPNLQuery,
  getDailyRPNLQuery,
  getConstrainedPositionsCTE,
}
