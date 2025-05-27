'use strict'

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
            from: { type: 'integer', minimum: 0 },
            to: { type: 'integer', minimum: 0 }
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
        const { rows } = await client.query(
          `
            SELECT
              day,
              denom,
              sum(daily_delta) OVER (
                PARTITION BY address, denom
                ORDER BY day
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
              ) AS ending_balance
            FROM daily_balances
            WHERE address = $1
            ORDER BY day;
          `, [request.params.address],
        )
        console.log(rows)
        return { coins: rows }
      } finally {
        client.release()
      }
    }
  )
}
