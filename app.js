'use strict'

const path = require('node:path')
const AutoLoad = require('@fastify/autoload')
const cors = require('@fastify/cors')

// Pass --options via CLI arguments in command to enable these options.
const options = {}

module.exports = async function (fastify, opts) {
  // Place here your custom code!

  await fastify.register(cors, {
    origin: (origin, cb) => {
      if (!origin) {
        // Allow non-browser tools like curl, Postman
        cb(null, true)
        return
      }

      try {
        const { hostname } = new URL(origin)
        if (hostname === 'localhost' || hostname === '127.0.0.1') {
          cb(null, true)
          return
        }

        if (
          ['dem.exchange', 'celeris.exchange'].some(allowedHost =>
            hostname === allowedHost || hostname.endsWith(`.${allowedHost}`)
          )
        ) {
          cb(null, true)
          return
        }
      } catch (e) {
        // Bad origin format → block it
        cb(new Error('Not allowed by CORS'), false)
      }

      cb(new Error('Not allowed by CORS'), false)
    }
  })

  // Do not touch the following lines

  // This loads all plugins defined in plugins
  // those should be support plugins that are reused
  // through your application
  fastify.register(AutoLoad, {
    dir: path.join(__dirname, 'src', 'plugins'),
    options: Object.assign({}, opts)
  })

  // This loads all plugins defined in routes
  // define your routes in one of these
  fastify.register(AutoLoad, {
    dir: path.join(__dirname, 'src', 'routes'),
    options: Object.assign({}, opts)
  })
}

module.exports.options = options
