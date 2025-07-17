const { daysAgo } = require('../../helpers/time');

module.exports = async function (fastify, opts) {
  fastify.get('/volume/:address', {
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
            to: { type: 'string', format: 'date-time' },
            denom: { type: 'string' }
          },
          additionalProperties: false
        },
        response: {
          200: {
            type: 'object',
            properties: {
              volume: {
                type: 'array',
                items: {
                  type: 'object',
                  required: ['day', 'maker_amount', 'taker_amount', 'total_amount'],
                  properties: {
                    day: { type: 'string', format: 'date-time' },
                    maker_amount: { type: 'string' },
                    taker_amount: { type: 'string' },
                    total_amount: { type: 'string' },
                    denom: { type: 'string' }
                  },
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
        const from = request.query.from || daysAgo(30).toDateString()
        const to = request.query.to || daysAgo(0).toDateString()
        const denom = request.query.denom

        const query = `
          SELECT
            day,
            ${denom ? '' : 'denom,'}
            maker_amount * (10 ^ -decimals)::decimal AS maker_amount,
            taker_amount * (10 ^ -decimals)::decimal AS taker_amount,
            total_amount * (10 ^ -decimals)::decimal AS total_amount
          FROM
          (
            SELECT
              day,
              value_denom AS denom,
              SUM(maker_total_value) AS maker_amount,
              SUM(taker_total_value) AS taker_amount,
              SUM(maker_total_value) + SUM(taker_total_value) AS total_amount
            FROM
              (
                  SELECT
                    day,
                    total_value AS taker_total_value,
                    0 AS maker_total_value,
                    value_denom
                  FROM daily_taker_summary WHERE address = $1 AND day >= $2 AND day <= $3 ${denom ? 'AND value_denom = $4' : ''}
                  UNION
                  SELECT
                    day,
                    0 AS taker_total_value,
                    total_value AS maker_total_value,
                    value_denom
                  FROM daily_maker_summary WHERE address = $1 AND day >= $2 AND day <= $3 ${denom ? 'AND value_denom = $4' : ''}
              ) daily_summary
            GROUP BY day, value_denom
            ORDER BY day DESC, value_denom ASC
          ) volumes
          LEFT JOIN tokens ON tokens.denom = volumes.denom;
        `

        const params = [request.params.address, from, to]
        if (denom) params.push(denom)

        const { rows } = await client.query(query, params)
        return { volume: rows }
      } finally {
        client.release()
      }
    }
  )

  fastify.get('/fees/:address', {

  }, async function (request, reply) {
    return {}
  })
}