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

module.exports = {
  getFeesQuery,
}


