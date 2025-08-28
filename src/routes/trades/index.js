const { normalizedTimeParams, daysAgo } = require('../../helpers/time');
const { getVolumeQuery, getFeesQuery, getFundingQuery } = require('../../queries/trades');
const { getOpenPositionUPnl, TotalRPNLQuery } = require('../../queries/positions');

const ONE_DAY = 24 * 60 * 60 * 1000

module.exports = async function (fastify, opts) {
  fastify.get('/perps_pnl/:address', {
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
          },
          additionalProperties: false
        },
        response: {
          200: {
            type: 'object',
            properties: {
              address: { type: 'string' },
              from: { type: 'string', format: 'date-time' },
              to: { type: 'string', format: 'date-time' },
              pnls: {
                type: 'array',
                items: {
                  type: 'object',
                  required: ['day', 'pnl'],
                  properties: {
                    day: { type: 'string', format: 'date-time' },
                    pnl: { type: 'number' },
                  },
                  additionalProperties: false
                }
              }
            }
          }
        }
      }
  }, async function (request, reply) {
     const client = await fastify.pg.connect()
      try {
        const { address } = request.params
        const { from, to } = normalizedTimeParams(request.query)

        const { rows } = await client.query(TotalRPNLQuery, [address, from, to])
        const upnl = await getOpenPositionUPnl(client, address)

        // gapfill
        const date = new Date(from)
        const filled = []
        if (rows.length > 0) {
          let row = rows.shift()
          while (rows.length) {
            if (new Date(row.day).getTime() === date.getTime()) {
              filled.push({ day: row.day.toISOString(), pnl: row.rpnl })
              row = rows.shift()
            } else {
              filled.push({ day: date.toISOString(), pnl: 0 })
            }
            date.setDate(date.getDate() + 1)
          }
          filled.push({ day: row.day.toISOString(), pnl: (Number(row.rpnl) + upnl).toString() })
        } else {
          filled.push({ day: daysAgo(0).toISOString(), pnl: upnl.toString() })
        }

        return { address, from, to, pnls: filled }
      } finally {
        client.release()
      }
  })

  fastify.get('/volume/:address?', {
      schema: {
        params: {
          type: 'object',
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
              address: { type: 'string' },
              from: { type: 'string', format: 'date-time' },
              to: { type: 'string', format: 'date-time' },
              volume: {
                type: 'array',
                items: {
                  type: 'object',
                  required: ['day', 'maker_amount', 'taker_amount', 'total_amount'],
                  properties: {
                    day: { type: 'string', format: 'date-time' },
                    address: { type: 'string' },
                    maker_amount: { type: 'string' },
                    taker_amount: { type: 'string' },
                    total_amount: { type: 'string' },
                    denom: { type: 'string' }
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
        const { address } = request.params
        const denom = request.query.denom
        const { from, to } = normalizedTimeParams(request.query)
        const [query, params] = getVolumeQuery({ address, from, to, denom })
        const { rows } = await client.query(query, params)
        return { address, from, to, volume: rows }
      } finally {
        client.release()
      }
    }
  )

  fastify.get('/fees/:address?', {
    schema: {
      params: {
        type: 'object',
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
            address: { type: 'string' },
            from: { type: 'string', format: 'date-time' },
            to: { type: 'string', format: 'date-time' },
            fees: {
              type: 'array',
              items: {
                type: 'object',
                required: ['day',
                  'taker_fee', 'taker_fee_kickback', 'taker_fee_commission',
                  'maker_fee', 'maker_fee_kickback', 'maker_fee_commission',
                  'total_fee', 'total_fee_kickback', 'total_fee_commission'
                ],
                properties: {
                  address: { type: 'string' },
                  day: { type: 'string', format: 'date-time' },
                  denom: { type: 'string' },
                  taker_fee: { type: 'number' },
                  taker_fee_kickback: { type: 'number' },
                  taker_fee_commission: { type: 'number' },
                  maker_fee: { type: 'number' },
                  maker_fee_kickback: { type: 'number' },
                  maker_fee_commission: { type: 'number' },
                  total_fee: { type: 'number' },
                  total_fee_kickback: { type: 'number' },
                  total_fee_commission: { type: 'number' },
                },
                additionalProperties: false
              }
            }
          }
        }
      }
    }
  }, async function (request, reply) {
      const client = await fastify.pg.connect()
      try {
        const { address } = request.params
        const { denom } = request.query
        const { from, to } = normalizedTimeParams(request.query)

        const [query, params] = getFeesQuery({ address, denom, from, to })

        const { rows } = await client.query(query, params)

        return { address, from, to, fees: rows }
      } finally {
        client.release()
      }
  })


  fastify.get('/funding/:address', {
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
            address: { type: 'string' },
            from: { type: 'string', format: 'date-time' },
            to: { type: 'string', format: 'date-time' },
            funding: {
              type: 'array',
              items: {
                type: 'object',
                required: ['market', 'amount'],
                properties: {
                  time: { type: 'string', format: 'date-time' },
                  market: { type: 'string' },
                  amount: { type: 'string' },
                },
                additionalProperties: false
              }
            }
          }
        }
      }
    }
  }, async function (request, reply) {
      const client = await fastify.pg.connect()
      try {
        const { address } = request.params
        const { from, to } = normalizedTimeParams(request.query)

        const bucket = (to.getTime() - from.getTime()) <= ONE_DAY ? 'hour' : 'day'
        const query = getFundingQuery(true, bucket)

        const { rows } = await client.query(query, [address, from, to])

        return { address, from, to, funding: rows }
      } finally {
        client.release()
      }
  })
}
