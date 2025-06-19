#!/bin/bash

set -e

export NVM_DIR="/home/ubuntu/.nvm"

if [ -s "$NVM_DIR/nvm.sh" ]; then
  echo "Sourcing nvm.sh..."
  . "$NVM_DIR/nvm.sh"
else
  echo "Error: nvm.sh not found or empty at $NVM_DIR/nvm.sh"
  exit 1
fi

if nvm use default; then
  echo "Switched to default Node.js version"
else
  echo "Error: Failed to use default Node.js version. Is a default version set?"
  exit 1
fi

if [ -f "package.json" ]; then
  echo "Running npm install..."
  npm install
  echo "npm install completed successfully"
else
  echo "Error: package.json not found in current directory"
  exit 1
fi

pm2 startOrReload ecosystem.config.js --env $1

echo "Installed successfully"
