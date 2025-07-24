'use strict'

const { bech32 } = require('bech32');
const { createHash } = require('crypto');
const { getBalanceQuery } = require('../../queries/balances');
const { getOpenPositionPnls } = require('../../queries/positions');
const { getFeesQuery } = require('../../queries/trades');
const { daysAgo, today } = require('../../helpers/time');

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
        const from = request.query.from || daysAgo(30)
        const to = request.query.to || today()
        const address = generatePerpPoolAddress(id)
        const [query, params] = getBalanceQuery(address, { denom: 'cgt/1', from, to })
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

        const { rows: supplies } = await client.query(SupplyQuery, [`cplt/${id}`, from, to])

        const days = (new Date(to) - new Date(from)) / (1000 * 60 * 60 * 24)
        if (days < 1) {
          throw new Error('Time frame needs to be > 1 day')
        }

        if (balances.length === 0 || supplies.length === 0) {
          throw new Error(`Could not find pool with id '${id}' and address '${address}'`)
        }

        if (balances[0].day.toString() !== supplies[0].day.toString()) {
          throw new Error(`Found first balance ${balances[0].day} but first supply ${supplies[0].day}`)
        }

        if (balances.length < 2) {
          throw new Error('Insufficient data')
        }

        if (balances[balances.length - 1].day.toString() !== supplies[supplies.length - 1].day.toString()) {
          throw new Error(`Found first balance ${balances[balances.length - 1].day} but first supply ${supplies[supplies.length - 1].day}`)
        }

        const { upnl } = await getOpenPositionPnls(client, address)
        const initialPrice = parseFloat(balances[0].ending_balance) / parseFloat(supplies[0].ending_total_supply)
        const finalPrice = (parseFloat(balances[balances.length - 1].ending_balance) + upnl) / parseFloat(supplies[supplies.length - 1].ending_total_supply)
        const apr = (finalPrice - initialPrice) / initialPrice / days * 365

        return { id, address, from, to, days, initialPrice, finalPrice, apr }
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
        let { from, to } = request.query
        from ||= daysAgo(30)
        from = daysAgo(1, from).toDateString() // we need one additional day to derive pnl for the first day
        to ||= daysAgo(0).toDateString()

        const denom = 'cgt/1'
        const address = generatePerpPoolAddress(id) // TODO: handle spot?
        const [query, balanceParams] = getBalanceQuery(address, { denom, from, to })
        const balanceQuery = `
          SELECT
            day,
            ending_balance * (10 ^ -18)::decimal AS ending_balance
          FROM (
            ${query}
            ORDER BY day ASC
          ) t;
        `
        const [feeQuery, feeParams] = getFeesQuery(address, { denom, from, to })
        const { rows: balanceRows } = await client.query(balanceQuery, balanceParams)
        const { rows: supplyRows } = await client.query(SupplyQuery, [`cplt/${id}`, from, to])
        const { rows: feeRows } = await client.query(feeQuery, feeParams)
        const { rows: totalRPNLRows } = await client.query(TotalRPNLQuery, [address, from, to])

        const dates = supplyRows.map(elem => elem.day)
        const balances = toDateMap(balanceRows)
        const supplies = toDateMap(supplyRows)
        const fees = toDateMap(feeRows)
        const rpnls = toDateMap(totalRPNLRows)

        const { upnl, rpnl } = await getOpenPositionPnls(client, address)

        if (dates.length < 2) {
          throw new Error('Insufficient data')
        }

        const result = []
        for (let i = 1; i < dates.length; ++i) {
          const day = dates[i]
          const prevDay = dates[i-1]
          const isLast = i === dates.length - 1
          let totalProfit = parseFloat(balances[day].ending_balance) - parseFloat(balances[prevDay].ending_balance)
          // account for deposits / withdrawals
          const inflow = parseFloat(supplies[day].ending_total_supply) - parseFloat(supplies[prevDay].ending_total_supply)
          if (inflow !== 0) {
            // assume inflow $ is at prev value
            const prevValue = parseFloat(balances[prevDay].ending_balance) / parseFloat(supplies[prevDay].ending_total_supply)
            totalProfit = totalProfit - (inflow * prevValue)
          }
          if (isLast) {
            totalProfit += upnl
          }
          const rPNL = parseFloat(rpnls[day]?.total_realized_pnl || 0) + (isLast ? rpnl : 0)
          const fee = parseFloat(fees[day]?.total_fee || 0)
          // since totalProfit = rpnl + funding - fees, =>
          const funding = totalProfit - rPNL + fee
          result.push({ day, totalPNL: totalProfit, components: { rPNL, uPNL: isLast ? upnl : 0, feeRebate: -fee, funding }})
        }

        return { address, performance: result }
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
            required: ['id', 'address', 'taker_amount', 'maker_amount', 'total_amount'],
            properties: {
              id: { type: 'string' },
              address: { type: 'string' },
              taker_amount: { type: 'number' },
              maker_amount: { type: 'number' },
              total_amount: { type: 'number' },
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

        return { id, address, ...rows[0]}
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
    const d = elem.day
    delete elem.day
    map[d] = elem
    return map
  }, {})
}

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
            WHERE day >= '2019-01-01' -- must be from start of all data to carry-forward values properly
            AND day <= $3
            GROUP BY (time_bucket_gapfill('1 day', day))
          ) filled
          WHERE day >= $2
          ORDER BY day ASC;
        `

const TotalRPNLQuery = `
  WITH h AS (
    SELECT
      f.hour,
      -- f.address,
      f.market,
      CASE
         WHEN p.closed_block_height != 0 THEN
          -- since we are using snapshots, minus of the previous rpnl which has either been closed and accounted for in 'hourly_closed_rpnl',
          -- or is being carried to this current open position which we should net off from previous snapshot
          p.realized_pnl - lead(p.realized_pnl, 1, 0) OVER (PARTITION BY f.address, f.market ORDER BY f.hour DESC)
        ELSE 0 -- if this is a closed position, it is already fully accounted for in 'hourly_closed_rpnl' below
      END AS rpnl
    FROM hourly_final_position_ids f
    JOIN archived_positions p ON p.id = f.id
    WHERE f.address = $1
  ),
  j AS (
    SELECT
      c.hour,
      -- c.address,
      SUM(COALESCE(c.total_realized_pnl, 0)) + SUM(COALESCE(h.rpnl, 0)) AS rpnl
    FROM hourly_closed_rpnl c
    FULL OUTER JOIN h ON c.hour = h.hour -- AND c.address = h.address
    WHERE c.address = $1
    GROUP BY c.hour --, c.address
  )
  SELECT
    time_bucket_gapfill('1 day', j.hour) AS day,
    SUM(j.rpnl) AS rpnl
  FROM j
  WHERE j.hour > $2 AND j.hour < $3
  GROUP BY day
  ORDER BY day DESC;
`
