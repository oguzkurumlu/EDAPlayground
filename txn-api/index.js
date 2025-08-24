import express from "express";
import pkg from "pg";

const { Pool } = pkg;

const {
  PORT = 8080,
  PGHOST = "localhost",
  PGPORT = 5432,
  PGUSER = "debezium",
  PGPASSWORD = "debezium",
  PGDATABASE = "corebank"
} = process.env;

const pool = new Pool({
  host: PGHOST,
  port: Number(PGPORT),
  user: PGUSER,
  password: PGPASSWORD,
  database: PGDATABASE,
});

const app = express();
app.use(express.json());

app.get("/health", (_, res) => res.json({ ok: true }));

// POST /transactions 
app.post("/transactions", async (req, res) => {
  const {
    account_id = 1,
    txn_type = "DEBIT", // "CREDIT" veya "DEBIT"
    amount = Number((Math.random() * 100).toFixed(2)),
    currency = "TRY",
    description = "Inserted via txn-api"
  } = req.body || {};

  if (!["DEBIT", "CREDIT"].includes(txn_type)) {
    return res.status(400).json({ error: "txn_type must be DEBIT or CREDIT" });
  }
  if (!account_id || isNaN(Number(amount))) {
    return res.status(400).json({ error: "account_id and numeric amount are required/valid" });
  }

  const sql = `
    INSERT INTO public.transactions (account_id, txn_type, amount, currency, description)
    VALUES ($1, $2, $3, $4, $5)
    RETURNING txn_id, account_id, txn_type, amount, currency, description, created_at
  `;

  try {
    const { rows } = await pool.query(sql, [
      account_id, txn_type, amount, currency, description
    ]);
    res.status(201).json({ inserted: rows[0] });
  } catch (err) {
    console.error("insert-failed", err);
    res.status(500).json({ error: "insert_failed", detail: err.message });
  }
});

app.listen(PORT, () => {
  console.log(`txn-api listening on :${PORT}`);
});
