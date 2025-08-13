'use strict'

const { RPC_BASE_URL, HYDROGEN_BASE_URL, cachedFetch } = require('../helpers/fetch');

const PRICE_CACHE = new Map()
const MARKET_CACHE = new Map()

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

  return cache
}

module.exports = {
  getTokenPrices,
  getMarketPrices,
}
