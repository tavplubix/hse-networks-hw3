#!/usr/bin/env bash

docker-compose -f setup/docker-compose.yml up -d

pip3 install -r setup/requirements.txt

sleep 20

docker-compose -f setup/docker-compose.yml exec db psql -h localhost -p 5432 -U postgres -f /create_script/schema_desc.sql

echo "Now you can run server.py"
