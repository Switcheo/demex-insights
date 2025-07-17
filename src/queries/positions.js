'use strict'

const { getTokenPrices } = require('./prices');

// getOpenPositions returns the currently open positions for the given address along with the position's underlying token denom
async function getOpenPositions(client, address) {
  const query = `
    SELECT
      positions.entry_price * (10 ^ (markets.base_precision - markets.quote_precision)) AS entry_price,
      positions.lots * (10 ^ -markets.base_precision)::decimal AS lots,
      markets.base AS token
    FROM open_positions
    INNER JOIN positions ON positions.id = open_positions.id
    INNER JOIN markets ON markets.id = open_positions.market
    WHERE open_positions.address = $1
    ;
  `

  const { rows: positions } = await client.query(query, [address])
  return positions
}

async function getUnrealizedPnl(client, address) {
  const prices = await getTokenPrices()
  const positions = await getOpenPositions(client, address)

  let upnl = 0.0
  for (const p of positions) {
    const mark_price = prices.get(p['token'])
    console.log(p['token'], mark_price)

    if (!mark_price || !mark_price.price) continue

    upnl += parseFloat(p['lots']) * (parseFloat(mark_price.price) - parseFloat(p['entry_price']))
  }

  return upnl
}

module.exports = {
  getOpenPositions,
  getUnrealizedPnl,
}
