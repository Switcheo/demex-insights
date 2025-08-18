'use strict'

// getBalanceQuery returns the query fragment for the daily ending balance of each denom for the given address
function getFeesQuery(address, { denom = null, from, to }) {
  const params = [address, from, to]
  if (denom) params.push(denom)

 const query = `
    SELECT
      day,
      ${denom ? '' : 'fees.denom AS denom,'}
      taker_fee * (10 ^ -decimals)::decimal AS taker_fee,
      taker_fee_kickback * (10 ^ -decimals)::decimal AS taker_fee_kickback,
      taker_fee_commission * (10 ^ -decimals)::decimal AS taker_fee_commission,
      maker_fee * (10 ^ -decimals)::decimal AS maker_fee,
      maker_fee_kickback * (10 ^ -decimals)::decimal AS maker_fee_kickback,
      maker_fee_commission * (10 ^ -decimals)::decimal AS maker_fee_commission,
      total_fee * (10 ^ -decimals)::decimal AS total_fee,
      total_fee_kickback * (10 ^ -decimals)::decimal AS total_fee_kickback,
      total_fee_commission * (10 ^ -decimals)::decimal AS total_fee_commission
    FROM
    (
      SELECT
        day,
        fee_denom AS denom,
        SUM(total_taker_fee) AS taker_fee,
        SUM(total_taker_fee_kickback) AS taker_fee_kickback,
        SUM(total_taker_fee_commission) AS taker_fee_commission,
        SUM(total_maker_fee) AS maker_fee,
        SUM(total_maker_fee_kickback) AS maker_fee_kickback,
        SUM(total_maker_fee_commission) AS maker_fee_commission,
        SUM(total_taker_fee) + SUM(total_maker_fee) AS total_fee,
        SUM(total_taker_fee_kickback) + SUM(total_maker_fee_kickback) AS total_fee_kickback,
        SUM(total_taker_fee_commission) + SUM(total_maker_fee_commission) AS total_fee_commission
      FROM
        (
            SELECT
              day,
              total_fee AS total_taker_fee,
              total_fee_kickback AS total_taker_fee_kickback,
              total_fee_commission AS total_taker_fee_commission,
              0 AS total_maker_fee,
              0 AS total_maker_fee_kickback,
              0 AS total_maker_fee_commission,
              fee_denom
            FROM daily_taker_summary WHERE address = $1 AND day >= $2 AND day <= $3 ${denom ? 'AND fee_denom = $4' : ''}
            UNION
            SELECT
              day,
              0 AS total_taker_fee,
              0 AS total_taker_fee_kickback,
              0 AS total_taker_fee_commission,
              total_fee AS total_maker_fee,
              total_fee_kickback AS total_maker_fee_kickback,
              total_fee_commission AS total_maker_fee_commission,
              fee_denom
            FROM daily_maker_summary WHERE address = $1 AND day >= $2 AND day <= $3 ${denom ? 'AND fee_denom = $4' : ''}
        ) daily_summary
      GROUP BY day, fee_denom
      ORDER BY day DESC, fee_denom ASC
    ) fees
    LEFT JOIN tokens ON tokens.denom = fees.denom;
  `

  return [query, params]
}

const getFundingQuery = (byMarket = false, bucketInterval = 'day') => `
  WITH p AS (
    SELECT
      *
    FROM archived_positions
    WHERE archived_positions.updated_block_height >= (
      SELECT MIN(block_height) AS min_height
      FROM blocks
      WHERE blocks.time >= GREATEST($2, (NOW() - INTERVAL '91 days'))
    )
    AND address = $1
  ),
  f AS (
    SELECT
      p.address,
      p.market,
      p.update_reason,
      p.lots,
      time_bucket(INTERVAL '1 ${bucketInterval}', b.time) AS time,
      realized_pnl - LAG(realized_pnl) OVER (PARTITION BY p.address, p.market ORDER BY p.updated_block_height ASC) AS rpnl_delta
    FROM p
    JOIN blocks b ON b.block_height = p.updated_block_height
  )
  SELECT
    f.time,
    ${byMarket ? ' f.market, ' : ''}
    SUM(f.rpnl_delta) * -(10 ^ -18) as amount -- inverse as +ve pnl is a rebate, and we want to show payment amounts
  FROM f
  WHERE f.update_reason = 6
  AND f.time >= $2 AND f.time <= $3
  GROUP BY f.time ${byMarket ? ', f.market' : ''}
  ORDER BY f.time DESC ${byMarket ? ', f.market ASC' : ''};
`

module.exports = {
  getFeesQuery,
  getFundingQuery
}


