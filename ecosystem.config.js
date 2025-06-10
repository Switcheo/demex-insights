module.exports = {
  apps : [{
    name: 'demex-insights',
    script: './node_modules/.bin/fastify',
    args: 'start -l info app.js',
    instances: 'max',
    exec_mode: 'fork',
    env: {
      NODE_ENV: 'development',
      PORT: 3000,
    },
    env_production: {
      NODE_ENV: 'production',
      PORT: 30040,
    },
    watch: false,
    autorestart: true,
    time: true,
  }],
  deploy : {
    production : {
      user : 'ubuntu',
      host : '203.118.10.75',
      port : '30000',
      ref  : 'origin/master',
      repo : 'git@github.com:Switcheo/demex-insights.git',
      path : '/home/ubuntu/demex-insights',
      'pre-deploy-local': '',
      'post-deploy' : 'npm install && pm2 reload ecosystem.config.js --env production',
      'pre-setup': ''
    }
  }
};
