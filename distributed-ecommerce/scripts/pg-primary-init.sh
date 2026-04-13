#!/bin/bash
# Creates additional shard databases on the primary instance
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE orders_shard1;
    CREATE DATABASE orders_shard2;
    GRANT ALL PRIVILEGES ON DATABASE orders_shard1 TO $POSTGRES_USER;
    GRANT ALL PRIVILEGES ON DATABASE orders_shard2 TO $POSTGRES_USER;
EOSQL
