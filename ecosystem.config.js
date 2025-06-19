module.exports = {
  apps : [{
    name: 'demex-insights',
    script: './node_modules/.bin/fastify',
    args: 'start -l info app.js',
    instances: '4',
    exec_mode: 'cluster',
    append_env_to_name: true,
    env: {
      NODE_ENV: 'development',
      PORT: 3000,
    },
    env_staging: {
      NODE_ENV: 'production',
      PORT: 30310,
      FASTIFY_ADDRESS: '0.0.0.0',
      DATABASE_URL: 'postgres://carboninsights@192.168.68.88:5432/carbon'
    },
    env_production: {
      NODE_ENV: 'production',
      PORT: 30300,
      FASTIFY_ADDRESS: '0.0.0.0',
      DATABASE_URL: 'postgres://carboninsights@192.168.68.89:5432/carbon'
    },
    watch: false,
    autorestart: true,
    time: true,
  }],
  deploy : {
    staging : {
      user : 'ubuntu',
      host : '203.118.10.75',
      port : '30000',
      ref  : 'origin/master',
      repo : 'git@github.com:Switcheo/demex-insights.git',
      path : '/home/ubuntu/demex-insights-staging',
      'post-deploy' : './scripts/post-deploy.sh staging',
    },
    production : {
      user : 'ubuntu',
      host : '203.118.10.75',
      port : '30000',
      ref  : 'origin/master',
      repo : 'git@github.com:Switcheo/demex-insights.git',
      path : '/home/ubuntu/demex-insights',
      'post-deploy' : './scripts/post-deploy.sh production',
    }
  }
};
