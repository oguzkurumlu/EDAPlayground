import { Kafka, logLevel } from "kafkajs";

const {
  KAFKA_BROKERS = "localhost:9092",
  KAFKA_CLIENT_ID = "alerts-consumer",
  KAFKA_GROUP_ID = "alerts-reader",
  KAFKA_TOPIC = "alerts.duplicate_amount",
  FROM_BEGINNING = "false"
} = process.env;

const kafka = new Kafka({
  clientId: KAFKA_CLIENT_ID,
  brokers: KAFKA_BROKERS.split(",").map(s => s.trim()),
  logLevel: logLevel.ERROR
});

const consumer = kafka.consumer({ groupId: KAFKA_GROUP_ID, allowAutoTopicCreation: false });

const log = (msg, obj = {}) =>
  console.log(JSON.stringify({ ts: new Date().toISOString(), msg, ...obj }));

async function run() {
  await consumer.connect();
  log("consumer-connected", { brokers: KAFKA_BROKERS });

  await consumer.subscribe({
    topic: KAFKA_TOPIC,
    fromBeginning: FROM_BEGINNING.toLowerCase() === "true"
  });
  log("subscribed", { topic: KAFKA_TOPIC });

  await consumer.run({
    autoCommit: true,
    eachMessage: async ({ topic, partition, message }) => {
      const raw = message.value?.toString() ?? "";
      let alert;
      try { alert = JSON.parse(raw); }
      catch { log("parse-error", { raw: raw.slice(0, 200) }); return; }

      const { account_id, cnt, window_start, window_end, last_amount } = alert;

      console.log(
        JSON.stringify({
          ts: new Date().toISOString(),
          event: "duplicate-alert",
          topic,
          partition,
          offset: message.offset,
          account_id,
          cnt,
          last_amount,
          window: { start: window_start, end: window_end }
        })
      );
    }
  });

  const shutdown = async (sig) => {
    try { log("shutdown", { sig }); await consumer.disconnect(); }
    finally { process.exit(0); }
  };
  ["SIGINT", "SIGTERM"].forEach(s => process.on(s, () => shutdown(s)));
}

run().catch(err => {
  console.error("fatal", err);
  process.exit(1);
});
