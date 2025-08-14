'use strict'

const { normalizedTimeParams } = require('../../helpers/time')
const { getBalanceQuery } = require('../../queries/balances')
const { getTokenPrices } = require('../../queries/prices')

module.exports = async function (fastify, opts) {
  fastify.get('/coins/:address', {
      schema: {
        params: {
          type: 'object',
          required: ['address'],
          properties: {
            address: { type: 'string' }
          },
          additionalProperties: false
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
                  required: ['day', 'denom', 'ending_balance'],
                  properties: {
                    day: { type: 'string', format: 'date-time' },
                    denom: { type: 'string' },
                    ending_balance: { type: 'string' }
                  },
                  additionalProperties: false
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
        const { from, to } = normalizedTimeParams(request.query)
        const [query, params] = getBalanceQuery([request.params.address], { from, to })
        const sortedQuery = `
          ${query}
          ORDER BY day ASC, denom ASC;
        `
        const { rows } = await client.query(sortedQuery, params)
        return { coins: rows }
      } finally {
        client.release()
      }
    }
  )

  fastify.get('/values/:address', {
      schema: {
        params: {
          type: 'object',
          required: ['address'],
          properties: {
            address: { type: 'string' }
          },
          additionalProperties: false
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
            additionalProperties: false,
            properties: {
              values: {
                type: 'array',
                items: {
                  type: 'object',
                  required: ['day', 'ending_value'],
                  properties: {
                    day: { type: 'string', format: 'date-time' },
                    denom: { type: 'string' },
                    ending_value: { type: 'string' }
                  },
                  additionalProperties: false
                }
              }
            },
          }
        }
      }
    },
    async function (request, reply) {
      const client = await fastify.pg.connect()
      try {
        const { from, to } = request.query
        const prices = await getTokenPrices()
        const values = Array.from(prices.entries()).map(([denom, value]) => `('${denom}', ${value?.price || 0}, ${value?.decimals || 18})`).join(', ');

        const [query, params] = getBalanceQuery([request.params.address], { from, to })
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
          ORDER BY day ASC;
        `

        const { rows } = await client.query(valueQuery, params)
        return { values: rows }
      } finally {
        client.release()
      }
    }
  )

  fastify.get('/whales', {
      schema: {
        params: {
          type: 'object',
          properties: {},
          additionalProperties: false
        },
        querystring: {
          type: 'object',
          properties: {
            from: { type: 'string', format: 'date-time' },
            to: { type: 'string', format: 'date-time' },
            denom: { type: 'string' },
            limit: { type: 'number' }
          },
          additionalProperties: false
        },
        response: {
          200: {
            type: 'array',
            items: {
              type: 'object',
              required: ['date', 'whales'],
              additionalProperties: false,
              properties: {
                date: { type: 'string', format: 'date-time' },
                whales: {
                  type: 'array',
                  items: {
                    type: 'object',
                    required: ['address', 'total_value'],
                    additionalProperties: false,
                    properties: {
                      address: { type: 'string' },
                      total_value: { type: 'number' },
                      assets: {
                        type: 'array',
                        items: {
                          type: 'object',
                          required: ['denom', 'amount', 'value'],
                          additionalProperties: false,
                          properties: {
                            denom: { type: 'string' },
                            amount: { type: 'string' },
                            value: { type: 'number' },
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    },
    async function (request, reply) {
    }
  )
}
