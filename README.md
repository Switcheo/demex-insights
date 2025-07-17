# Demex Insights

This project was bootstrapped with Fastify-CLI.

## Development

In the project directory, you can run:

### `npm run dev`

To start the app in dev mode.\
Open [http://localhost:3000](http://localhost:3000) to view it in the browser.

### `npm run dev -- -p 4000`

To start in a different port.

### `BECH32_PREFIX=swth DATABASE_URL=postgres://carboninsights:<yourpw>@localhost:5433/carbon npm run dev -- -p 4000`

To start the app on port 4000, and set a custom database url / bech32 prefix.
Use this in combination with the ssh port forward command to connect to a remote db via ssh:

```bash
# where:
# 192.168.68.88:20009 is where the db is hosted
# 5433 is the port on your local host
# ubuntu@203.118.10.75 -p 30000 is the ssh host and port
ssh -L 5433:192.168.68.88:20009 ubuntu@203.118.10.75 -p 30000
```

### `npm run test`

Run the test cases.

### `npm start`

To run in production mode.

## Deployment

Deployment is done through [PM2](https://pm2.keymetrics.io/docs/usage/pm2-doc-single-page/).

Install it first with:

```bash
$ npm install pm2@latest -g
# or
$ yarn global add pm2
```

Deploy with `pm2 deploy production`.

To set up a new environment, use `pm2 deploy <env> setup`

## Fastify CLI Cheatsheet

```bash
# Setup
npm install fastify-cli --global
# See routes
npx fastify routes --help
```
