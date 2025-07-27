CREATE OR REPLACE PROCEDURE dm.fill_f101_round_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP := clock_timestamp();
    v_end_time TIMESTAMP;
    v_duration_sec DOUBLE PRECISION;
    v_row_count INTEGER := 0;

    v_FromDate DATE := (i_OnDate - INTERVAL '1 month')::DATE;
    v_ToDate DATE := (i_OnDate - INTERVAL '1 day')::DATE;
    v_BalanceStartDate DATE := (v_FromDate - INTERVAL '1 day')::DATE;
BEGIN
    -- Удаляем старые записи за указанный период
    DELETE FROM dm.dm_f101_round_f
    WHERE from_date = v_FromDate AND to_date = v_ToDate;

    -- Вставка рассчитанных данных
    INSERT INTO dm.dm_f101_round_f (
        from_date, to_date, chapter, ledger_account, characteristic,
        balance_in_rub, balance_in_val, balance_in_total,
        turn_deb_rub, turn_deb_val, turn_deb_total,
        turn_cre_rub, turn_cre_val, turn_cre_total,
        balance_out_rub, balance_out_val, balance_out_total
    )
    SELECT
        v_FromDate,
        v_ToDate,
        l.chapter,
        LEFT(a.account_number, 5) AS ledger_account,
        a.char_type AS characteristic,

        -- Остатки на начало
        SUM(CASE WHEN a.currency_code IN ('643', '810') THEN b_in.balance_out_rub ELSE 0 END) AS balance_in_rub,
        SUM(CASE WHEN a.currency_code NOT IN ('643', '810') THEN b_in.balance_out_rub ELSE 0 END) AS balance_in_val,
        SUM(b_in.balance_out_rub) AS balance_in_total,

        -- Дебет
        SUM(CASE WHEN a.currency_code IN ('643', '810') THEN t.debet_amount_rub ELSE 0 END) AS turn_deb_rub,
        SUM(CASE WHEN a.currency_code NOT IN ('643', '810') THEN t.debet_amount_rub ELSE 0 END) AS turn_deb_val,
        SUM(t.debet_amount_rub) AS turn_deb_total,

        -- Кредит
        SUM(CASE WHEN a.currency_code IN ('643', '810') THEN t.credit_amount_rub ELSE 0 END) AS turn_cre_rub,
        SUM(CASE WHEN a.currency_code NOT IN ('643', '810') THEN t.credit_amount_rub ELSE 0 END) AS turn_cre_val,
        SUM(t.credit_amount_rub) AS turn_cre_total,

        -- Остатки на конец
        SUM(CASE WHEN a.currency_code IN ('643', '810') THEN b_out.balance_out_rub ELSE 0 END) AS balance_out_rub,
        SUM(CASE WHEN a.currency_code NOT IN ('643', '810') THEN b_out.balance_out_rub ELSE 0 END) AS balance_out_val,
        SUM(b_out.balance_out_rub) AS balance_out_total

    FROM ds.md_account_d a
    JOIN ds.md_ledger_account_s l
        ON LEFT(a.account_number, 5)::INT = l.ledger_account
    LEFT JOIN dm.dm_account_balance_f b_in
        ON b_in.account_rk = a.account_rk AND b_in.on_date = v_BalanceStartDate
    LEFT JOIN dm.dm_account_balance_f b_out
        ON b_out.account_rk = a.account_rk AND b_out.on_date = v_ToDate
    LEFT JOIN (
        SELECT
            account_rk,
            SUM(debet_amount_rub) AS debet_amount_rub,
            SUM(credit_amount_rub) AS credit_amount_rub
        FROM dm.dm_account_turnover_f
        WHERE on_date BETWEEN v_FromDate AND v_ToDate
        GROUP BY account_rk
    ) t ON t.account_rk = a.account_rk
    WHERE v_FromDate BETWEEN a.data_actual_date AND a.data_actual_end_date
    GROUP BY l.chapter, LEFT(a.account_number, 5), a.char_type;

    -- Подсчёт строк
    GET DIAGNOSTICS v_row_count = ROW_COUNT;

    -- Время окончания и лог
    v_end_time := clock_timestamp();
    v_duration_sec := EXTRACT(EPOCH FROM v_end_time - v_start_time);

    INSERT INTO logs.data_load_log (
        file_name, load_date, start_time, end_time,
        duration_sec, count_row, error_message
    )
    VALUES (
        'dm_f101_round_f',
        CURRENT_DATE,
        v_start_time,
        v_end_time,
        v_duration_sec,
        v_row_count,
        NULL
    );

EXCEPTION
    WHEN OTHERS THEN
        v_end_time := clock_timestamp();
        v_duration_sec := EXTRACT(EPOCH FROM v_end_time - v_start_time);

        INSERT INTO logs.data_load_log (
            file_name, load_date, start_time, end_time,
            duration_sec, count_row, error_message
        )
        VALUES (
            'dm_f101_round_f',
            CURRENT_DATE,
            v_start_time,
            v_end_time,
            v_duration_sec,
            0,
            SQLERRM
        );
        RAISE;
END;
$$;
