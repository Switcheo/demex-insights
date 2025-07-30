'use strict'

function daysAgo(days = 7, from = today()) {
  const ago = new Date(from)
  ago.setDate(new Date(from).getDate() - days)
  return ago
}

function today() {
  const now = new Date();
  return new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
}

function removeTime(datetime) {
  return new Date(datetime.getFullYear(), datetime.getMonth(), datetime.getDate());
}

// `normalizedTimeParams` normalizes the query params input for standard time parameters,
// to give a normalized, formatted js Date, or the defaults if none were provided.
// `fromOffset` allows us to add one additional day to the from date, which is required
// to derive position pnls for the first day due to its cumulative nature.
function normalizedTimeParams(query, fromOffset = 0) {
    const { from: f, to: t } = query
    const from = f ? daysAgo(0 + fromOffset, f) : daysAgo(30 + fromOffset)
    const to = daysAgo(0, t)
    return { from, to }
}

module.exports = {
  daysAgo,
  today,
  removeTime,
  normalizedTimeParams
}
