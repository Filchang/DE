create table ds.ft_balance_f (
    on_date date not null,
    account_rk numeric not null,
    currency_rk numeric,
    balance_out numeric,
    primary key (on_date, account_rk)
);

create table ds.ft_posting_f (
    oper_date date not null,
    credit_account_rk numeric not null,
    debet_account_rk numeric not null,
    credit_amount numeric,
    debet_amount numeric
);

create table ds.md_account_d (
    data_actual_date date not null,
    data_actual_end_date date not null,
    account_rk numeric not null,
    account_number varchar(20) not null,
    char_type varchar(1) not null,
    currency_rk numeric not null,
    currency_code varchar(3) not null,
    primary key (data_actual_date, account_rk)
);

create table ds.md_currency_d (
    currency_rk numeric not null,
    data_actual_date date not null,
    data_actual_end_date date,
    currency_code varchar(3),
    code_iso_char varchar(2),
    primary key (currency_rk, data_actual_date)
);

create table ds.md_exchange_rate_d (
    data_actual_date date not null,
    data_actual_end_date date,
    currency_rk numeric not null,
    reduced_cource numeric,
    code_iso_num varchar(3),
    primary key (data_actual_date, currency_rk)
);

create table ds.md_ledger_account_s (
    chapter char(1),
    chapter_name varchar(16),
    section_number integer,
    section_name varchar(22),
    subsection_name varchar(21),
    ledger1_account integer,
    ledger1_account_name varchar(47),
    ledger_account integer not null,
    ledger_account_name varchar(153),
    characteristic char(1),
    is_resident integer,
    is_reserve integer,
    is_reserved integer,
    is_loan integer,
    is_reserved_assets integer,
    is_overdue integer,
    is_interest integer,
    pair_account varchar(5),
    start_date date not null,
    end_date date,
    is_rub_only integer,
    min_term varchar(1),
    min_term_measure varchar(1),
    max_term varchar(1),
    max_term_measure varchar(1),
    ledger_acc_full_name_translit varchar(1),
    is_revaluation varchar(1),
    is_correct varchar(1),
    primary key (ledger_account, start_date)
);
