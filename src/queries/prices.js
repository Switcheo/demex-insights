'use strict'

const PRICE_CACHE = new Map()
const MARKET_CACHE = new Map()

const RPC_BASE_URL = process.env.NODE_BASE_URL || 'https://test-api.carbon.network'
const HYDROGEN_BASE_URL = process.env.HYDROGEN_BASE_URL || 'https://test-hydrogen-api.carbon.network'

async function getTokenPrices() {
  return cachedFetch(PRICE_CACHE, fetchTokenPrices)
}

async function getMarketPrices() {
  return cachedFetch(MARKET_CACHE, fetchMarketPrices)
}

async function fetchTokenPrices(cache) {
  const response = await fetch(`${HYDROGEN_BASE_URL}/tokens?limit=5000`);

  if (!response.ok) {
    throw new Error(`HTTP fetch error! status: ${response.status}`);
  }

  const json = await response.json();

  for (const item of json['data']) {
    cache.set(item['denom'], { price: item['price_usd'], decimals: item['decimals'] })
  }

  cache.set('__last_hydrate_time__', (new Date()).getTime())

  return cache
}

async function fetchMarketPrices(cache) {
  const response = await fetch(`${RPC_BASE_URL}/carbon/pricing/v1/prices`);

  if (!response.ok) {
    throw new Error(`HTTP fetch error! status: ${response.status}`);
  }

  const json = await response.json();

  for (const item of json['prices']) {
    cache.set(item['market_id'], item['mark'])
  }

  cache.set('__last_hydrate_time__', (new Date()).getTime())

  return cache
}

async function cachedFetch(cache, fetch) {
  const hydrated_at = cache.get('__last_hydrate_time__')
  if (!!hydrated_at) {
    // cache available
    if (hydrated_at + 1*60*1000 < (new Date()).getTime()) {
      // rehydrate every 1 minute
      console.info("Rehydrating cache..")
      fetch(cache)
    }
    // use cache
    return cache
  }

  try {
    console.warn("No token price cache available, loading now..")
    return fetch(cache)
  } catch (error) {
    console.error('Fetch error:', error);
  }
}

module.exports = {
  getTokenPrices,
  getMarketPrices,
}
