'use strict'

function getVolumeQuery({ address = null, denom = null, from, to }) {
  const params = [from, to]
  if (denom) params.push(denom)
  if (address) params.push(address)

  const query = `
    SELECT
      day,
      ${address ? '' : 'volumes.address,'}
      ${denom ? '' : 'volumes.denom,'}
      maker_amount * (10 ^ -decimals)::decimal AS maker_amount,
      taker_amount * (10 ^ -decimals)::decimal AS taker_amount,
      total_amount * (10 ^ -decimals)::decimal AS total_amount
    FROM
    (
      SELECT
        day,
        ${address ? '' : 'address,'}
        value_denom AS denom,
        SUM(maker_total_value) AS maker_amount,
        SUM(taker_total_value) AS taker_amount,
        SUM(maker_total_value) + SUM(taker_total_value) AS total_amount
      FROM
        (
          SELECT
            day,
            address,
            total_value AS taker_total_value,
            0 AS maker_total_value,
            value_denom
          FROM daily_taker_summary WHERE day >= $1 AND day <= $2 ${denom ? 'AND value_denom = $3' : ''} ${address ? 'AND address = $'+params.length  : ''}
          UNION
          SELECT
            day,
            address,
            0 AS taker_total_value,
            total_value AS maker_total_value,
            value_denom
          FROM daily_maker_summary WHERE day >= $1 AND day <= $2 ${denom ? 'AND value_denom = $3' : ''} ${address ? 'AND address = $'+params.length : ''}
        ) daily_summary
      GROUP BY day ${address ? '' : ', address'}, value_denom
      ORDER BY day DESC ${address ? '' : ', address ASC'}, value_denom ASC
    ) volumes
    LEFT JOIN tokens ON tokens.denom = volumes.denom;
  `
  console.log(query)

  return [query, params]
}

// getFeesQuery returns the query fragment for the fees for the given address
function getFeesQuery({ address = null, denom = null, from, to }) {
  const params = [from, to]
  if (denom) params.push(denom)
  if (address) params.push(address)

 const query = `
    SELECT
      day,
      ${address ? '' : 'address,'}
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
        ${address ? '' : 'address,'}
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
              address,
              total_fee AS total_taker_fee,
              total_fee_kickback AS total_taker_fee_kickback,
              total_fee_commission AS total_taker_fee_commission,
              0 AS total_maker_fee,
              0 AS total_maker_fee_kickback,
              0 AS total_maker_fee_commission,
              fee_denom
            FROM daily_taker_summary WHERE day >= $1 AND day <= $2 ${denom ? 'AND fee_denom = $3' : ''} ${address ? 'AND address = $'+params.length  : ''}
            UNION
            SELECT
              day,
              address,
              0 AS total_taker_fee,
              0 AS total_taker_fee_kickback,
              0 AS total_taker_fee_commission,
              total_fee AS total_maker_fee,
              total_fee_kickback AS total_maker_fee_kickback,
              total_fee_commission AS total_maker_fee_commission,
              fee_denom
            FROM daily_maker_summary WHERE day >= $1 AND day <= $2 ${denom ? 'AND fee_denom = $3' : ''} ${address ? 'AND address = $'+params.length  : ''}
        ) daily_summary
      GROUP BY day ${address ? '' : ', address'}, fee_denom
      ORDER BY day DESC ${address ? '' : ', address ASC'}, fee_denom ASC
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
  getVolumeQuery,
  getFeesQuery,
  getFundingQuery
}
