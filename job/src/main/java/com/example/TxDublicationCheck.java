package com.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class TxDublicationCheck {

  public static final class Tx {
    public long txnId;
    public long accountId;
    public String txnType;
    public double amount;
    public String currency;
    public Tx() {}
    public Tx(long txnId, long accountId, String txnType, double amount, String currency) {
      this.txnId = txnId; this.accountId = accountId; this.txnType = txnType;
      this.amount = amount; this.currency = currency;
    }
  }

  public static void main(String[] args) throws Exception {
    final String brokers = System.getProperty("brokers", System.getenv().getOrDefault("brokers", "kafka:29092"));
    final String inTopic = System.getProperty("in",       System.getenv().getOrDefault("in", "transactions"));
    final String outTopic= System.getProperty("out",      System.getenv().getOrDefault("out", "alerts.duplicate_amount"));
    final String groupId = System.getProperty("group",    System.getenv().getOrDefault("group", "flink-tx-window-1"));

    System.out.printf("[BOOT] brokers=%s in=%s out=%s groupId=%s%n", brokers, inTopic, outTopic, groupId);

    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(10_000);

    var source = KafkaSource.<String>builder()
        .setBootstrapServers(brokers)
        .setTopics(inTopic)
        .setGroupId(groupId)
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .build();

    DataStream<String> raw = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-transactions")
        .map(s -> {
          int len = (s == null ? 0 : s.length());
          System.out.printf("[RAW] %d bytes%n", len);
          if (len > 0) {
            String sample = s.substring(0, Math.min(400, len)).replaceAll("\\s+"," ");
            System.out.printf("[RAW-SAMPLE] %s%n", sample);
          }
          return s;
        }).name("raw-debug");

    ObjectMapper mapper = new ObjectMapper();

    DataStream<Tx> events = raw.flatMap((String s, org.apache.flink.util.Collector<Tx> out) -> {
      try {
        if (s == null || s.isBlank()) { System.out.println("[PARSE-SKIP] empty/null"); return; }

        JsonNode root = mapper.readTree(s);
        JsonNode container = root.has("payload") ? root.get("payload") : root;

        JsonNode row;
        String path;
        if (container.has("after") && !container.get("after").isNull()) {
          row = container.get("after"); path = "payload.after";
        } else if (root.has("after") && !root.get("after").isNull()) {
          row = root.get("after"); path = "after";
        } else if (root.has("data") && !root.get("data").isNull()) {
          row = root.get("data"); path = "data";
        } else {
          row = container; path = "top-level";
        }

        String op = container.path("op").asText("");
        if ("d".equals(op)) { System.out.println("[PARSE-SKIP] op=d"); return; }

        if (row == null || row.isNull() || !row.isObject()) {
          String sample = root.toString(); if (sample.length() > 200) sample = sample.substring(0,200);
          System.out.printf("[PARSE-SKIP] row null/non-object path=%s sample=%s%n", path, sample);
          return;
        }

        var it = row.fieldNames(); StringBuilder keys = new StringBuilder(); int c = 0;
        while (it.hasNext() && c < 10) { if (c++ > 0) keys.append(','); keys.append(it.next()); }
        System.out.printf("[PATH] %s keys=%s hasPayload=%s hasAfter(container)=%s op=%s%n",
            path, keys, root.has("payload"), container.has("after"), op);

        long txnId     = row.path("txn_id").asLong(0L);
        long accountId = row.path("account_id").asLong(Long.MIN_VALUE);
        String txnType = row.path("txn_type").asText("");
        double amount  = row.path("amount").asDouble(0.0);
        String currency= row.path("currency").asText("TRY");

        if (accountId == Long.MIN_VALUE) {
          String sample = row.toString(); if (sample.length() > 200) sample = sample.substring(0,200);
          System.out.printf("[PARSE-SKIP] account_id yok; path=%s sample=%s%n", path, sample);
          return;
        }

        Tx tx = new Tx(txnId, accountId, txnType, amount, currency);
        System.out.printf("[EVT] account=%d amount=%.2f type=%s cur=%s op=%s path=%s%n",
            tx.accountId, tx.amount, tx.txnType, tx.currency, op, path);

        out.collect(tx);

      } catch (Exception e) {
        String sample = s; if (sample != null && sample.length() > 200) sample = sample.substring(0,200);
        System.out.printf("[PARSE-ERR] %s | sample=%s%n", e.getMessage(), sample);
      }
    }).returns(Tx.class).name("parse");

    var alerts = events
        .keyBy(tx -> String.valueOf(tx.accountId))
        .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
        .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(5)))
        .process(new CountAndAlert())
        .name("window-count-alert");

    var sink = KafkaSink.<String>builder()
        .setBootstrapServers(brokers)
        .setRecordSerializer(
            KafkaRecordSerializationSchema.builder()
                .setTopic(outTopic)
                .setValueSerializationSchema(new SimpleStringSchema())
                .build())
        .build();

    alerts
        .map(s -> { System.out.println("[OUT] " + s); return s; })
        .name("stdout-out")
        .sinkTo(sink)
        .name("kafka-sink");

    env.execute("tx-stream-window-30s");
  }

  static class CountAndAlert extends ProcessWindowFunction<Tx, String, String, TimeWindow> {
    @Override
    public void process(String key, Context ctx, Iterable<Tx> elements, Collector<String> out) {
      long cnt = 0;
      long accountId = -1;
      double lastAmount = 0;

      for (Tx e : elements) {
        cnt++;
        accountId = e.accountId;
        lastAmount = e.amount;
      }

      long ws = ctx.window().getStart();
      long we = ctx.window().getEnd();

      System.out.printf("[INFO] window key=%s cnt=%d window=[%d-%d]%n", key, cnt, ws, we);

      if (cnt >= 2) {
        System.out.printf("[ALERT] account=%d cnt=%d window=[%d-%d]%n", accountId, cnt, ws, we);
        out.collect(String.format(
            "{\"account_id\":%d,\"cnt\":%d,\"window_start\":%d,\"window_end\":%d,\"last_amount\":%.2f}",
            accountId, cnt, ws, we, lastAmount));
      }
    }
  }
}
