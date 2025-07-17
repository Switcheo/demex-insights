'use strict'

function daysAgo(days = 7, from = today()) {
  const ago = new Date(from)
  ago.setDate(from.getDate() - days)
  return ago
}

function today() {
  const now = new Date();
  return new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
}

function removeTime(datetime) {
  return new Date(datetime.getFullYear(), datetime.getMonth(), datetime.getDate());
}

module.exports = {
  daysAgo,
  today,
  removeTime
}
