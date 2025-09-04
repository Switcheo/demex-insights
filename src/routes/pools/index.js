'use strict'

const { bech32 } = require('bech32');
const { createHash } = require('crypto');
const { getBalanceQuery } = require('../../queries/balances');
const { getOpenPositionUPnl, getRPNLQuery } = require('../../queries/positions');
const { getFeesQuery, getFundingQuery } = require('../../queries/trades');
const { normalizedTimeParams, today, daysAgo } = require('../../helpers/time');
const { cachedFetch, RPC_BASE_URL } = require('../../helpers/fetch');

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
              days: { type: 'number' },
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

        const [query, params] = getBalanceQuery([address], { denom: 'cgt/1', from, to, startDate })
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

        const [query, params] = getFeesQuery({ address, denom, from, to })

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

        const [feeQuery, feeParams] = getFeesQuery({ address, denom, from, to })
        const [rpnlQuery, rpnlParams] = getRPNLQuery({ address, from, to })
        const { rows: feeRows } = await client.query(feeQuery, feeParams)
        const { rows: fundingRows } = await client.query(getFundingQuery(), [address, from, to])
        const { rows: supplyRows } = await client.query(SupplyQuery, [poolDenom, from, to, startDate])
        const { rows: totalPNLRows } = await client.query(rpnlQuery, rpnlParams)

        const firstDateIdx = supplyRows.findIndex(r => r.ending_total_supply !== '0')
        const dates = supplyRows.map(elem => elem.day.toISOString()).slice(firstDateIdx)
        const fees = toMap(feeRows, 'day')
        const fundings = toMap(fundingRows, 'time')
        const pnls = toMap(totalPNLRows, 'time')

        const upnl = await getOpenPositionUPnl(client, address)

        if (dates.length < 2) {
          throw new Error('Insufficient data')
        }

        const result = []
        for (let i = 1; i < dates.length; ++i) {
          const day = dates[i]
          const isLast = i === dates.length - 1
          const funding = parseFloat(fundings[day]?.amount || 0)
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


  // gets 7d, 14d, 30d apy and 24h volume for all perp / user pools
  fastify.get('/stats', {
     schema: {
        response: {
          200: {
            type: 'object',
            properties: {
              stats: {
                type: 'array',
                items: {
                  type: 'object',
                  required: ['id', 'address', 'volume', 'aprs'],
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
                      },
                      additionalProperties: false
                    },
                    aprs: {
                      type: 'array',
                      items: {
                        type: 'object',
                        required: ['timeframe', 'from', 'activeDays', 'to', 'initialPrice', 'finalPrice',  'apr'],
                        properties: {
                          timeframe: { type: 'number' },
                          activeDays: { type: 'number' },
                          from: { type: 'string', format: 'date-time' },
                          to: { type: 'string', format: 'date-time' },
                          initialPrice: { type: 'number' },
                          finalPrice: { type: 'number' },
                          apr: { type: 'number' },
                        },
                        additionalProperties: false
                      }
                    },
                  },
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
        // get all volume last 24 hr
        const volumeQuery = `
          WITH m AS (
            SELECT
              maker_address AS address,
              SUM(quantity * price) * (10 ^ -18)::decimal AS maker_amount
            FROM archived_trades
            WHERE
              block_created_at > NOW() - INTERVAL '24 hours'
            GROUP BY maker_address
          ), t AS (
            SELECT
              taker_address AS address,
              SUM(quantity * price) * (10 ^ -18)::decimal AS taker_amount
            FROM archived_trades
            WHERE
              block_created_at > NOW() - INTERVAL '24 hours'
            GROUP BY taker_address
          )
          SELECT
            COALESCE(m.address, t.address) AS address,
            COALESCE(maker_amount, 0) AS maker_amount,
            COALESCE(taker_amount, 0) AS taker_amount,
            COALESCE(maker_amount, 0) + COALESCE(taker_amount, 0) AS total_amount
          FROM
            m FULL OUTER JOIN t ON m.address = t.address
          ORDER BY address ASC;
        `
        const { rows: volumeRows } = await client.query(volumeQuery)
        const volumes = toMap(volumeRows, 'address')

        const pools = Array.from((await getPoolDenomsAndStartDates(client)).values())
        const minStartDate = pools.reduce((min, item) =>  new Date(item.startDate) < new Date(min.startDate) ? item : min, pools[0]).startDate;
        const addresses = pools.map(item => item.poolAddress)
        const denoms = pools.map(item => item.poolDenom)

        const to = today()
        const from = daysAgo(30, to)
        const [query, params] = getBalanceQuery([addresses], { denom: 'cgt/1', from, to, minStartDate })
        const balanceQuery = `
          SELECT
            day,
            address,
            ending_balance * (10 ^ -18)::decimal AS ending_balance
          FROM (
            ${query}
            ORDER BY address, day ASC
          ) t;
        `
        const { rows: balanceRows } = await client.query(balanceQuery, params)
        const balances = toMap(balanceRows, 'address', 'day')

        const { rows: supplyRows } = await client.query(MultiSupplyQuery, [denoms])
        const supplies = toMap(supplyRows, 'denom', 'day')

        const results = []
        for (const pool of pools) {
          const { id, poolAddress: address, poolDenom: denom, startDate } = pool
          const v = volumes[address]
          const volume = { takerAmount: v?.taker_amount || '0', makerAmount: v?.maker_amount || '0', totalAmount: v?.total_amount || '0' }
          const r = { id, address, volume, aprs: [] }

          const timeframes = [30, 14, 7]
          for (let days of timeframes) {
            const upnl = await getOpenPositionUPnl(client, address)
            const { finalPrice, finalDate } = finalPriceAndDate(address, denom, balances, supplies, startDate, to, upnl)

            // get the initial price and date by getting the final price from start of pool to start of timeframe
            let startOfFrame = daysAgo(days, to)
            if (startOfFrame < startDate) {
              startOfFrame = startDate
            }
            const { finalPrice: initialPrice, finalDate: initialDate } = finalPriceAndDate(address, denom, balances, supplies, startDate, startOfFrame, 0)

            if (finalPrice) {
              const activeDays = (finalDate - initialDate) / (1000 * 60 * 60 * 24)
              const apr = (finalPrice - initialPrice) / initialPrice / days * 365

              r.aprs.push({ timeframe: days, from: startOfFrame, to, activeDays, initialPrice, finalPrice, apr, ...r })
            }
          }

          results.push(r)
        }

        return { stats: results }
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
function generatePerpPoolAddress(poolId, prefix = (process.env.BECH32_PREFIX || `${process.env.CARBON_ENV === 'mainnet' ? '' : 't'}swth`)) {
  const input = Buffer.from(`${PerpsPoolVaultName}${poolId}`, 'utf8');
  const hashed = addressHash(input);

  // Convert to 5-bit words for bech32 encoding
  const words = bech32.toWords(hashed);
  return bech32.encode(prefix, words);
}

// Converts an array into a nested object with keys given by keyNames
// e.g.
// # toMap([{ id: 1, address: 'swth1', a: 1, b: 2 }], ['id', 'address'])
// #  =>
// # { 1: { 'swth1': { a: 1, b: 2 } }}
function toMap(arr, ...keyNames) {
  const keys = Array.from(keyNames)
  return arr.reduce((map, elem) => {
    return rekey(map, elem, keys.slice())
  }, {})
}

function rekey(map, elem, keys) {
  const key = keys.shift()
  if (!key) throw new Error('No map key provided')

  let k = elem[key]
  delete elem[key]
  if (key === 'day' || key === 'time') {
    k = k.toISOString()
  }

  if (keys.length) {
    map[k] ||= {}
    rekey(map[k], elem, keys)
  } else {
    map[k] = elem
  }

  return map
}

function finalPriceAndDate(address, denom, balances, supplies, start, end, upnl) {
  const b = balances[address], s = supplies[denom]

  let date = new Date(end)
  let lastBalance, lastSupply, finalDate = undefined

  while (date >= start && b && s) {
    lastSupply = s[date.toISOString()]
    lastBalance ||= b[date.toISOString()]

    if (lastSupply && lastBalance === undefined) {
      throw new Error('Found a date with supply but no balance row')
    }

    if (lastBalance) {
      finalDate ||= date
    }

    if (lastBalance && lastSupply && lastSupply.ending_total_supply !== '0') {
      return { finalPrice: (parseFloat(lastBalance.ending_balance) + upnl) / parseFloat(lastSupply.ending_total_supply), finalDate }
    }

    date = daysAgo(1, date)
  }

  return { finalPrice: undefined, finalDate }
}

async function getPoolDenomAndStartDate(client, id) {
  const res = POOL_CACHE.get(id)
  if (!res) {
    await getPoolDenomsAndStartDates(client)
  }
  return POOL_CACHE.get(id)
}

const FETCH_CACHE = new Map()
const POOL_CACHE = new Map()
async function getPoolDenomsAndStartDates(client) {
  const result = await cachedFetch(FETCH_CACHE, fetchAllPoolDenoms)
  const { rows } = await client.query(PoolStartDateQuery, [Array.from(result.values())])
  for (const row of rows) {
    const id = row['denom'].split('/')[1]
    POOL_CACHE.set(id, { id, poolDenom: row['denom'], poolAddress: generatePerpPoolAddress(id), startDate: row['day'] } )
  }
  return POOL_CACHE
}

async function fetchAllPoolDenoms(cache) {
  const response = await fetch(`${RPC_BASE_URL}/carbon/perpspool/v1/pools/pool_info?pagination.limit=5000`);
  if (!response.ok) {
    throw new Error(`HTTP fetch error! status: ${response.status}`);
  }

  const response2 = await fetch(`${RPC_BASE_URL}/carbon/perpspool/v1/user_vaults_info?pagination.limit=5000`);
  if (!response2.ok) {
    throw new Error(`HTTP fetch error! status: ${response.status}`);
  }

  const json = await response.json();
  const json2 = await response2.json();

  for (const item of json['vaults']) {
    cache.set(item['id'], `cplt/${item['id']}`)
  }
  for (const item of json2['vaults']) {
    cache.set(item['id'], `duvt/${item['id']}`)
  }

  return cache
}

const PoolStartDateQuery = `
  SELECT
    denom,
    MIN(day) as day
  FROM daily_balances
  WHERE denom = ANY($1)
  GROUP BY denom;
`

// no gapfilling
const MultiSupplyQuery = `
  SELECT
    day,
    denom,
    total_daily_delta,
    SUM(total_daily_delta) OVER (
      PARTITION BY denom
      ORDER BY day
    ) AS ending_total_supply
  FROM (
    SELECT
      day,
      denom,
      SUM(daily_delta) AS total_daily_delta
    FROM daily_balances
    WHERE denom = ANY($1)
    GROUP BY day, denom
    ORDER BY day, denom
  ) ends
  ORDER BY denom, day;
`

// has gapfilling
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
