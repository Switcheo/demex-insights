'use strict'

const fp = require('fastify-plugin')

// the use of fastify-plugin is required to be able
// to export the decorators to the outer scope

module.exports = fp(async function (fastify, opts) {
  fastify.register(require('@fastify/postgres'), {
    connectionString: process.env.DATABASE_URL || 'postgres://ubuntu@localhost:5433/carbon'
  })
})
