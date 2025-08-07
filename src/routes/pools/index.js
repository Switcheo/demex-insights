'use strict'

const { bech32 } = require('bech32');
const { createHash } = require('crypto');
const { getBalanceQuery } = require('../../queries/balances');
const { getOpenPositionUPnl, TotalRPNLQuery } = require('../../queries/positions');
const { getFeesQuery } = require('../../queries/trades');
const { normalizedTimeParams } = require('../../helpers/time');

module.exports = async function (fastify, opts) {
  fastify.get('/apr/:id', {
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
            required: ['id', 'address', 'from', 'to', 'initialPrice', 'finalPrice', 'apr'],
            properties: {
              id: { type: 'string' },
              address: { type: 'string' },
              from: { type: 'string', format: 'date-time' },
              to: { type: 'string', format: 'date-time' },
              initialPrice: { type: 'number' },
              finalPrice: { type: 'number' },
              apr: { type: 'number' },
            },
            additionalProperties: false
          }
        }
      }
    },
    async function (request, reply) {
      const client = await fastify.pg.connect()
      try {
        const { id } = request.params
        const { from, to } = normalizedTimeParams(request.query)
        const address = generatePerpPoolAddress(id)
        const { poolDenom, startDate } = await getPoolDenomAndStartDate(client, id)
        if (!startDate) {
          throw new Error(`Cannot find pool with id: ${id} and denom: ${poolDenom}`)
        }

        const [query, params] = getBalanceQuery(address, { denom: 'cgt/1', from, to, startDate })
        const sortedQuery = `
          SELECT
            day,
            ending_balance * (10 ^ -18)::decimal AS ending_balance
          FROM (
            ${query}
            ORDER BY day ASC
          ) t;
        `
        const { rows: balances } = await client.query(sortedQuery, params)
        const { rows: supplies } = await client.query(SupplyQuery, [poolDenom, from, to, startDate])

        let start = new Date(startDate)
        if (from > start) {
          start = from
        }
        if (supplies.length < 2) {
          throw new Error('Insufficient data')
        }
        let end = new Date(supplies[supplies.length - 1].day)
        const days = (end - start) / (1000 * 60 * 60 * 24)

        if (balances[0].day.toString() !== supplies[0].day.toString()) {
          throw new Error(`Found first balance ${balances[0].day} but first supply ${supplies[0].day}`)
        }

        if (balances[balances.length - 1].day.toString() !== supplies[supplies.length - 1].day.toString()) {
          throw new Error(`Found first balance ${balances[balances.length - 1].day} but first supply ${supplies[supplies.length - 1].day}`)
        }

        const upnl = await getOpenPositionUPnl(client, address)
        let initialPrice = parseFloat(balances[0].ending_balance) / parseFloat(supplies[0].ending_total_supply)
        // if first price can't be found, it means the pool has no balance yet or it is empty, so just use 1
        if (!Number.isFinite(initialPrice)) initialPrice = 1

        // find the last valid price (because 0 supply would cause a div by zero), else use the initial price
        let finalPrice = initialPrice
        for (let i = supplies.length - 1; i >= 0; i--) {
          const p = (parseFloat(balances[balances.length - 1].ending_balance) + upnl) / parseFloat(supplies[supplies.length - 1].ending_total_supply)
          if (Number.isFinite(p)) {
            finalPrice = p
            break
          }
        }
        const apr = (finalPrice - initialPrice) / initialPrice / days * 365

        return { id, address, from: start, to: end, days, initialPrice, finalPrice, apr }
      } finally {
        client.release()
      }
    }
  )

  fastify.get('/fees/:id', {
    // TODO: add schema
  }, async function (request, reply) {
     const client = await fastify.pg.connect()
      try {
        const { id } = request.params
        const { denom, from, to } = request.query
        const address = generatePerpPoolAddress(id) // TODO: handle spot?

        const [query, params] = getFeesQuery(address, { denom, from, to })

        const { rows } = await client.query(query, params)

        return { fees: rows }
      } finally {
        client.release()
      }
  })

  fastify.get('/performance/:id', {
    // TODO: add schema
  }, async function (request, reply) {
     const client = await fastify.pg.connect()
      try {
        const { id } = request.params
        const { from, to } = normalizedTimeParams(request.query, 1)

        const denom = 'cgt/1'
        const address = generatePerpPoolAddress(id) // TODO: handle spot?
        const { poolDenom, startDate } = await getPoolDenomAndStartDate(client, id)
        if (!startDate) {
          throw new Error(`Cannot find pool with id: ${id} and denom: ${poolDenom}`)
        }

        const [feeQuery, feeParams] = getFeesQuery(address, { denom, from, to })
        const { rows: feeRows } = await client.query(feeQuery, feeParams)
        const { rows: fundingRows } = await client.query(FundingQuery, [address, from, to])
        const { rows: supplyRows } = await client.query(SupplyQuery, [poolDenom, from, to, startDate])
        const { rows: totalPNLRows } = await client.query(TotalRPNLQuery, [address, from, to])

        const firstDateIdx = supplyRows.findIndex(r => r.ending_total_supply !== '0')
        const dates = supplyRows.map(elem => elem.day.toISOString()).slice(firstDateIdx)
        const fees = toDateMap(feeRows)
        const fundings = toDateMap(fundingRows)
        const pnls = toDateMap(totalPNLRows)

        const upnl = await getOpenPositionUPnl(client, address)

        if (dates.length < 2) {
          throw new Error('Insufficient data')
        }

        const result = []
        for (let i = 1; i < dates.length; ++i) {
          const day = dates[i]
          const isLast = i === dates.length - 1
          const funding = parseFloat(fundings[day]?.funding || 0)
          const totalPNL = parseFloat(pnls[day]?.rpnl || 0)
          const makerFee = parseFloat(fees[day]?.maker_fee || 0)
          const takerFee = parseFloat(fees[day]?.taker_fee || 0)
          const totalFee = makerFee + takerFee
          const rPNL = totalPNL + totalFee + funding // fees and fundings are deductions
          result.push({ day, totalPNL, components: { rPNL, uPNL: isLast ? upnl : 0, makerFee, takerFee, fundingFee: funding }})
        }

        return { id, from, to, address, performance: result }
      } finally {
        client.release()
      }
  })

  fastify.get('/24h_volume/:id', {
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
            required: ['id', 'address', 'volume'],
            properties: {
              id: { type: 'string' },
              address: { type: 'string' },
              volume: {
                type: 'object',
                required: ['takerAmount', 'makerAmount', 'totalAmount'],
                properties: {
                  takerAmount: { type: 'number' },
                  makerAmount: { type: 'number' },
                  totalAmount: { type: 'number' }
                }
              }
            },
            additionalProperties: false
          }
        }
      }
  }, async function (request, reply) {
      const client = await fastify.pg.connect()
      try {
        const { id } = request.params
        const address = generatePerpPoolAddress(id) // TODO: handle spot?
        const query = `
          SELECT
            SUM(maker_amount) AS maker_amount,
            SUM(taker_amount) AS taker_amount,
            SUM(maker_amount) + SUM(taker_amount) AS total_amount
          FROM (
            SELECT
              SUM(quantity * price) * (10 ^ -18)::decimal AS maker_amount, --  // TODO: handle spot?
              0 AS taker_amount
            FROM archived_trades
            WHERE
              block_created_at > NOW() - INTERVAL '24 hours'
              AND maker_address = $1
            GROUP BY maker_address

            UNION

            SELECT
              0 AS maker_amount,
              SUM(quantity * price) * (10 ^ -18)::decimal AS taker_amount
            FROM archived_trades
            WHERE
              block_created_at > NOW() - INTERVAL '24 hours'
              AND taker_address = $1
            GROUP BY taker_address
          ) combined
          ;
        `
        const { rows } = await client.query(query, [address])

        if (rows.length === 0) {
          throw new Error(`Could not find any data for pool with id ${id}`)
        }

        const volume = {
          takerAmount: rows[0].taker_amount,
          makerAmount: rows[0].maker_amount,
          totalAmount: rows[0].total_amount
        }

        return { id, address, volume }
      } finally {
        client.release()
      }
  })
}

const PerpsPoolVaultName = 'perps_pool_vault';

// Mimics tmhash.SumTruncated: SHA-256 truncated to 20 bytes
function addressHash(inputBytes) {
  const hash = createHash('sha256').update(inputBytes).digest(); // returns Buffer
  return hash.subarray(0, 20); // 20-byte truncated hash
}

// Generates a Bech32-encoded address with dynamic prefix
function generatePerpPoolAddress(poolId, prefix = (process.env.BECH32_PREFIX || 'tswth')) {
  const input = Buffer.from(`${PerpsPoolVaultName}${poolId}`, 'utf8');
  const hashed = addressHash(input);

  // Convert to 5-bit words for bech32 encoding
  const words = bech32.toWords(hashed);
  return bech32.encode(prefix, words);
}

function toDateMap(arr) {
  return arr.reduce((map, elem) => {
    const d = elem.day.toISOString()
    delete elem.day
    map[d] = elem
    return map
  }, {})
}

async function getPoolDenomAndStartDate(client, id) {
  let poolDenom = `cplt/${id}`
  let startDate
  const { rows } = await client.query(PoolStartDateQuery, [poolDenom])
  if (!rows[0].day) {
    poolDenom = `duvt/${id}`
    const { rows: rows2 } = await client.query(PoolStartDateQuery, [poolDenom])
    startDate = rows2[0].day
  } else {
    startDate = rows[0].day
  }
  return { poolDenom, startDate }
}

const PoolStartDateQuery = `
  SELECT
    MIN(day) as day
  FROM daily_balances
  WHERE denom = $1;
`

const SupplyQuery = `
  SELECT
    day,
    COALESCE(ending_total_supply, 0) as ending_total_supply
  FROM (
    SELECT
    time_bucket_gapfill('1 day', day) AS day,
    locf(AVG(ending_total_supply)) AS ending_total_supply
    FROM (
      SELECT
        day,
        SUM(daily_delta) OVER (
          PARTITION BY denom
          ORDER BY day
        ) * (10 ^ -18)::decimal AS ending_total_supply
      FROM daily_balances
      WHERE denom = $1
    ) ends
    WHERE day >= $4
    AND day <= $3
    GROUP BY (time_bucket_gapfill('1 day', day))
  ) filled
  WHERE day >= $2
  ORDER BY day ASC;
`

const FundingQuery = `
  SELECT
    day,
    SUM(funding) AS funding
  FROM
    daily_funding
  WHERE address = $1 AND day >= $2 AND day <= $3
  GROUP BY address, day
  ORDER BY day ASC;
`