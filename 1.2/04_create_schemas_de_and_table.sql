DROP SCHEMA IF EXISTS dm CASCADE;

CREATE SCHEMA dm AUTHORIZATION ds_owner;

CREATE TABLE dm.dm_account_turnover_f (
    on_date DATE,
    account_rk NUMERIC,
    credit_amount NUMERIC(23,8),
    credit_amount_rub NUMERIC(23,8),
    debet_amount NUMERIC(23,8),
    debet_amount_rub NUMERIC(23,8)
);

CREATE TABLE dm.dm_account_balance_f (
    on_date DATE,
    account_rk NUMERIC,
    balance_out NUMERIC(23,8),
    balance_out_rub NUMERIC(23,8)
);

CREATE TABLE IF NOT EXISTS dm.dm_f101_round_f (
    from_date         DATE NOT NULL,
    to_date           DATE NOT NULL,
    chapter           char(1),
    ledger_account    char(5),
    characteristic    char(1),
    balance_in_rub    NUMERIC(23,8),
    balance_in_val    NUMERIC(23,8),
    balance_in_total  NUMERIC(23,8),
    turn_deb_rub      NUMERIC(23,8),
    turn_deb_val      NUMERIC(23,8),
    turn_deb_total    NUMERIC(23,8),
    turn_cre_rub      NUMERIC(23,8),
    turn_cre_val      NUMERIC(23,8),
    turn_cre_total    NUMERIC(23,8),
    balance_out_rub   NUMERIC(23,8),
    balance_out_val   NUMERIC(23,8),
    balance_out_total NUMERIC(23,8)
);

