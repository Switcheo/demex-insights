// getBalanceQuery returns the query fragment for the daily ending balance of each denom for the given address
export function getBalanceQuery(address, { denom = null, from = daysAgo(30).toDateString(), to = daysAgo(0).toDateString() }) {
  let where = `WHERE address = $1`
  const params = [address, from, to]
  if (denom) {
    where += ' AND denom = $4'
    params.push(denom)
  }

  const query = `
    SELECT
      day,
      denom,
      COALESCE(ending_balance, 0) as ending_balance
    FROM (
      SELECT
        time_bucket_gapfill('1 day', day) AS day,
        denom,
        locf(SUM(ending_balance)) AS ending_balance
      FROM (
        SELECT
          day,
          denom,
          SUM(daily_delta) OVER (
            PARTITION BY address, denom
            ORDER BY day
          ) AS ending_balance
        FROM daily_balances
        ${where}
      ) ends
      WHERE day >= '2019-01-01' -- must be from start of all data to carry-forward values properly
      AND day <= $3
      GROUP BY (time_bucket_gapfill('1 day', day), denom)
    ) filled
    WHERE day >= $2
  `
  return [query, params]
}

export function daysAgo(days = 7, from = today()) {
  const ago = new Date(from)
  ago.setDate(from.getDate() - days)
  return ago
}

export function today() {
  const now = new Date();
  return new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
}

export function removeTime(datetime) {
  return new Date(datetime.getFullYear(), datetime.getMonth(), datetime.getDate());
}
