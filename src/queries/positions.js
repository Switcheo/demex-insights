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

// getOpenPositionPnls returns the total unrealized and realized PNL of all open positions for the address
async function getOpenPositionPnls(client, address) {
  const prices = await getMarketPrices()
  const positions = await getOpenPositions(client, address)

  let upnl = 0.0
  let rpnl = 0.0
  for (const p of positions) {
    const raw_price = prices.get(p['market_id'])

    if (!raw_price) continue

    const mark_price = parseFloat(raw_price) * (10 ** parseInt(p['price_decimals'], 10))

    upnl += parseFloat(p['lots']) * (mark_price - parseFloat(p['entry_price']))
    rpnl += parseFloat(p['realized_pnl']) * (10 ** -18)
  }

  return { upnl, rpnl }
}

module.exports = {
  getOpenPositionPnls,
}
