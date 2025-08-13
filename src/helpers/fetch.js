'use strict'

const isMainnet = process.env.CARBON_ENV==='mainnet'
const RPC_BASE_URL = process.env.NODE_BASE_URL || `https://${isMainnet ? '' : 'test-'}api.carbon.network`
const HYDROGEN_BASE_URL = process.env.HYDROGEN_BASE_URL || `https://${isMainnet ? '' : 'test-'}hydrogen-api.carbon.network`

const HYDRATION_TIME_KEY = '__last_hydrate_time__'
async function cachedFetch(cache, fetch, expiry = 1*60*1000) { // rehydrate every 1 minute
  const now = new Date().getTime()
  const hydrated_at = cache.get(HYDRATION_TIME_KEY)

  if (!!hydrated_at) {
    // cache available
    if (hydrated_at + expiry < now) {
      cache.set(HYDRATION_TIME_KEY, now) // set first to avoid stampede on rehydration
      console.info("Rehydrating cache..")
      fetch(cache) // rehydrate but use previous cache
    }
  } else {
    console.warn("No cache available, loading now..")
    await fetch(cache) // wait for cache to fill
    cache.set(HYDRATION_TIME_KEY, now)
  }

  return cache
}

module.exports = {
  cachedFetch,
  RPC_BASE_URL,
  HYDROGEN_BASE_URL,
}
