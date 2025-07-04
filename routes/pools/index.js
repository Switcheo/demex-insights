'use strict'

const { bech32 } = require('bech32');
const { createHash } = require('crypto');
const { getBalanceQuery, daysAgo, today } = require('../../common');

module.exports = async function (fastify, opts) {
  fastify.get('/apr/:id', {
      schema: {
        params: {
          type: 'object',
          required: ['id'],
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
        // response: {
        //   200: {
        //     type: 'object',
        //     properties: {
        //       result: {
        //         type: 'string',
        //       }
        //     }
        //   }
        // }
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

        const { rows: supplies } = await client.query( `
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
        `, [`cplt/${id}`, from, to])

        const days = (new Date(to) - new Date(from)) / (1000 * 60 * 60 * 24)
        if (days < 1) {
          throw new Error('Time frame needs to be > 1 day')
        }

        if (balances.length === 0 || supplies.length === 0) {
          throw new Error('Could not find pool')
        }

        if (balances[0].day.toString() !== supplies[0].day.toString()) {
          throw new Error(`Found first balance ${balances[0].day} but first supply ${supplies[0].day}`)
        }

        if (balances[balances.length - 1].day.toString() !== supplies[supplies.length - 1].day.toString()) {
          throw new Error(`Found first balance ${balances[balances.length - 1].day} but first supply ${supplies[supplies.length - 1].day}`)
        }

        const initialPrice = parseFloat(balances[0].ending_balance) / parseFloat(supplies[0].ending_total_supply)
        const finalPrice = parseFloat(balances[balances.length - 1].ending_balance) / parseFloat(supplies[supplies.length - 1].ending_total_supply)
        const apr = (finalPrice - initialPrice) / initialPrice / days * 365

        return { id, address, from, to, days, initialPrice, finalPrice, apr }
      } finally {
        client.release()
      }
    }
  )
}

const PerpsPoolVaultName = 'perps_pool_vault';

// Mimics tmhash.SumTruncated: SHA-256 truncated to 20 bytes
function addressHash(inputBytes) {
  const hash = createHash('sha256').update(inputBytes).digest(); // returns Buffer
  return hash.subarray(0, 20); // 20-byte truncated hash
}

// Generates a Bech32-encoded address with dynamic prefix
function generatePerpPoolAddress(poolId, prefix = (process.env.BECH32_PREFIX || 'swth')) {
  const input = Buffer.from(`${PerpsPoolVaultName}${poolId}`, 'utf8');
  const hashed = addressHash(input);

  // Convert to 5-bit words for bech32 encoding
  const words = bech32.toWords(hashed);
  return bech32.encode(prefix, words);
}
