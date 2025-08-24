CREATE SCHEMA IF NOT EXISTS public;

-- Hesaplar
CREATE TABLE IF NOT EXISTS public.accounts (
  account_id BIGSERIAL PRIMARY KEY,
  iban TEXT UNIQUE,
  customer_id BIGINT,
  currency CHAR(3) NOT NULL DEFAULT 'TRY',
  balance NUMERIC(18,2) NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- İşlem hareketleri (INSERT ile satır ekleme = yeni hareket)
CREATE TABLE IF NOT EXISTS public.transactions (
  txn_id BIGSERIAL PRIMARY KEY,
  account_id BIGINT NOT NULL REFERENCES public.accounts(account_id),
  txn_type TEXT NOT NULL CHECK (txn_type IN ('CREDIT','DEBIT')),
  amount NUMERIC(18,2) NOT NULL CHECK (amount > 0),
  currency CHAR(3) NOT NULL,
  description TEXT,
  txn_time TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_transactions_account_time ON public.transactions(account_id, txn_time DESC);

-- Örnek hesaplar
INSERT INTO public.accounts (iban, customer_id, currency, balance)
VALUES ('TR00 0000 0000 0000 0000 0000 01', 1001, 'TRY', 1000.00),
       ('TR00 0000 0000 0000 0000 0000 02', 1002, 'USD',  250.00);

-- Örnek hareket (başlangıç)
INSERT INTO public.transactions (account_id, txn_type, amount, currency, description)
VALUES (1, 'CREDIT', 250.00, 'TRY', 'Initial deposit'),
       (2, 'DEBIT',  50.00,  'USD', 'Card purchase');
