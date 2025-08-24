import { Kafka, logLevel } from "kafkajs";

const {
  KAFKA_BROKERS = "localhost:9092",
  KAFKA_CLIENT_ID = "txn-consumer",
  KAFKA_GROUP_ID = "txn-writer",
  KAFKA_TOPIC = "transactions",
  FROM_BEGINNING = "false"
} = process.env;

const logger = (msg, obj = {}) => console.log(JSON.stringify({ ts: new Date().toISOString(), msg, ...obj }));

const kafka = new Kafka({
  clientId: KAFKA_CLIENT_ID,
  brokers: KAFKA_BROKERS.split(",").map(s => s.trim()),
  logLevel: logLevel.ERROR
});

const consumer = kafka.consumer({ groupId: KAFKA_GROUP_ID, allowAutoTopicCreation: false });

async function run() {
  await consumer.connect();
  logger("consumer-connected", { brokers: KAFKA_BROKERS });

  await consumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: FROM_BEGINNING.toLowerCase() === "true" });
  logger("subscribed", { topic: KAFKA_TOPIC });

  await consumer.run({
    autoCommit: true,
    eachMessage: async ({ topic, partition, message }) => {
      const key = message.key?.toString();
      const raw = message.value?.toString() ?? "";
      let json = null;
      try { json = raw ? JSON.parse(raw) : null; } catch {  }

      const record = {
        topic,
        partition,
        offset: message.offset,
        timestamp: message.timestamp,
        key,
        value: json ?? raw
      };

      console.log(record.value.payload)
    }
  });

  const shutdown = async (sig) => {
    try { logger("shutdown", { sig }); await consumer.disconnect(); }
    finally { process.exit(0); }
  };
  ["SIGINT", "SIGTERM"].forEach(s => process.on(s, () => shutdown(s)));
}

run().catch(err => {
  console.error("fatal", err);
  process.exit(1);
});

