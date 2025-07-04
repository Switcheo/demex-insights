'use strict'

const { getBalanceQuery } = require('../../common')

const PRICE_CACHE = new Map()

module.exports = async function (fastify, opts) {
  fastify.get('/coins/:address', {
      schema: {
        params: {
          type: 'object',
          required: ['address'],
          properties: {
            address: { type: 'string' }
          }
        },
        querystring: {
          type: 'object',
          properties: {
            from: { type: 'string', format: 'date-time' },
            to: { type: 'string', format: 'date-time' }
          },
          additionalProperties: false
        },
        response: {
          200: {
            type: 'object',
            properties: {
              coins: {
                type: 'array',
                items: {
                  type: 'object',
                  properties: {
                    day: { type: 'string', format: 'date-time' },
                    denom: { type: 'string' },
                    ending_balance: { type: 'string' }
                  },
                  required: ['day', 'denom', 'ending_balance']
                }
              }
            }
          }
        }
      }
    },
    async function (request, reply) {
      const client = await fastify.pg.connect()
      try {
        const [query, params] = getBalanceQuery(request.params.address, { from: request.query.from, to: request.query.to })
        const sortedQuery = `
          ${query}
          ORDER BY day DESC, denom ASC;
        `
        const { rows } = await client.query(sortedQuery, params)
        return { coins: rows }
      } finally {
        client.release()
      }
    }
  )

  fastify.get('/value/:address', {
      schema: {
        params: {
          type: 'object',
          required: ['address'],
          properties: {
            address: { type: 'string' }
          }
        },
        querystring: {
          type: 'object',
          properties: {
            from: { type: 'string', format: 'date-time' },
            to: { type: 'string', format: 'date-time' }
          },
          additionalProperties: false
        },
        response: {
          200: {
            type: 'object',
            properties: {
              values: {
                type: 'array',
                items: {
                  type: 'object',
                  properties: {
                    day: { type: 'string', format: 'date-time' },
                    denom: { type: 'string' },
                    ending_value: { type: 'string' }
                  },
                  required: ['day', 'ending_value']
                }
              }
            }
          }
        }
      }
    },
    async function (request, reply) {
      const client = await fastify.pg.connect()
      try {
        const prices = await getTokenPrices()
        const values = Array.from(prices.entries()).map(([denom, value]) => `('${denom}', ${value?.price || 0}, ${value?.decimals || 18})`).join(', ');

        const [query, params] = getBalanceQuery(request.params.address, { from: request.query.from, to: request.query.to })
        const valueQuery = `
          WITH
            prices(denom, price, decimals) AS (VALUES ${values}),
            ending_balances AS (${query})
          SELECT
            day,
            SUM(ending_balance * (10 ^ -decimals) * price) AS ending_value
          FROM prices
          INNER JOIN ending_balances
          ON prices.denom = ending_balances.denom
          GROUP BY day
          ORDER BY day DESC;
        `

        const { rows } = await client.query(valueQuery, params)
        return { values: rows }
      } finally {
        client.release()
      }
    }
  )
}

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
