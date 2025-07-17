'use strict'

const PRICE_CACHE = new Map()

async function getTokenPrices() {
  const hydrated_at = PRICE_CACHE.get('__last_hydrate_time__')
  if (!!hydrated_at) {
    // cache available
    if (hydrated_at + 5*60*1000 < (new Date()).getTime()) {
      // rehydrate every 5 minutes
      console.info("Rehydrating token price cache..")
      fetchTokenPrices()
    }
    // use cache
    return PRICE_CACHE
  }

  try {
    console.warn("No token price cache available, loading now..")
    return fetchTokenPrices()
  } catch (error) {
    console.error('Fetch error:', error);
  }
}

async function fetchTokenPrices() {
  const response = await fetch('https://hydrogen-api.carbon.network/tokens?limit=5000');

  if (!response.ok) {
    throw new Error(`HTTP fetch error! status: ${response.status}`);
  }

  const json = await response.json();

  for (const item of json['data']) {
    PRICE_CACHE.set(item['denom'], { price: item['price_usd'], decimals: item['decimals'] })
  }

  PRICE_CACHE.set('__last_hydrate_time__', (new Date()).getTime())

  return PRICE_CACHE
}

module.exports = {
  getTokenPrices,
}
