# Demex Insights

This project was bootstrapped with Fastify-CLI.

## Development

In the project directory, you can run:

### `npm run dev`

To start the app in dev mode.\
Open [http://localhost:3000](http://localhost:3000) to view it in the browser.

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
