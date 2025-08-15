'use strict'

const { normalizedTimeParams, today, daysAgo } = require('../../helpers/time');
const { getMarketPrices } = require('../../queries/prices');

module.exports = async function (fastify, opts) {
  fastify.get('/funding/:id', {
      schema: {
        params: {
          type: 'object',
          required: ['id'],
          properties: {
            id: { type: 'string' }
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
            required: ['id', 'from', 'to', 'funding'],
            properties: {
              id: { type: 'string' },
              from: { type: 'string', format: 'date-time' },
              to: { type: 'string', format: 'date-time' },
              funding: {
                type: 'array',
                items: {
                  type: 'object',
                  properties: {
                    time: { type: 'string', format: 'date-time' },
                    rate: { type: 'number' } // non-annualized
                  }
                }
              }
            },
            additionalProperties: false
          }
        }
      }
  }, async function (request, reply) {
    const client = await fastify.pg.connect()
    const prices = await getMarketPrices()
    try {
      const { id } = request.params
      const { from, to } = normalizedTimeParams(request.query)
      const min = daysAgo(30)
      const start = from < min ? min : from
      const end = to < from ? today() : to
      const marketID = Number.isNaN(parseInt(id, 10)) ? id : `cmkt/${id}`
      const { rows } = await client.query(FundingQuery, [marketID, start, end])

      const markPriceRaw = prices.get(marketID)
      if (!markPriceRaw) {
        throw new Error(`Can't find mark price for market`)
      }
      const markPrice = markPriceRaw * (10**-18)

      const funding = []
      for (const row of rows) {
        funding.push({ time: row.time, rate: row.total_funding / (row.total_longs * markPrice) })
      }

      return { id, from: start, to: end, funding }
    } finally {
      client.release()
    }
  })
}

const FundingQuery = `
  SELECT * from funding
  WHERE market = $1
  AND time >= $2 AND time <= $3
  ORDER BY market ASC, time ASC
;
`