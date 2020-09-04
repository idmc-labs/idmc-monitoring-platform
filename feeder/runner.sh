#!/bin/bash

echo "Setting up environment variables for cron"
printenv | sed 's/^\([a-zA-Z0-9_]*\)=\(.*\)$/export \1="\2"/g' > /root/project_env.sh
echo "Running cron"
cron -f
