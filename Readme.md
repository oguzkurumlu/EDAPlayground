## Kafka Connect definition

curl -s -X POST http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "pg-transactions-cdc",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "database.hostname": "postgres",
      "database.port": "5432",
      "database.user": "debezium",
      "database.password": "debezium",
      "database.dbname": "corebank",
      "topic.prefix": "pg",
      "plugin.name": "pgoutput",
      "schema.include.list": "public",
      "table.include.list": "public.transactions,public.accounts",
      "slot.name": "pg_cdc_slot",
      "publication.autocreate.mode": "filtered",
      "snapshot.mode": "initial",
      "tombstones.on.delete": "false",
      "transforms": "unwrap,route",
      "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
      "transforms.unwrap.add.fields": "op,source.ts_ms",
      "transforms.unwrap.delete.handling.mode": "rewrite",
      "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
      "transforms.route.regex": "([^.]+)\\.([^.]+)\\.([^.]+)",
      "transforms.route.replacement": "$3"
    }
  }'


## New transaction

  curl -X POST "http://localhost:8080/transactions" -H "content-type: application/json"   -d '{"account_id":1,"txn_type":"DEBIT","amount":99.99,"currency":"TRY"}'
