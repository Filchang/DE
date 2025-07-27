CREATE OR REPLACE PROCEDURE ds.fill_account_turnover_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP := clock_timestamp();
    v_end_time TIMESTAMP;
    v_duration DOUBLE PRECISION;
    v_row_count INTEGER := 0;
BEGIN
    DELETE FROM dm.dm_account_turnover_f
    WHERE on_date = i_OnDate;

    WITH
    account_currency AS (
        SELECT DISTINCT
            account_rk,
            currency_rk
        FROM ds.ft_balance_f
        WHERE on_date = i_OnDate
    ),
    rates AS (
        SELECT
            currency_rk,
            reduced_cource
        FROM ds.md_exchange_rate_d
        WHERE i_OnDate >= data_actual_date
          AND i_OnDate < data_actual_end_date
    ),
    credit AS (
        SELECT
            credit_account_rk AS account_rk,
            SUM(credit_amount) AS credit_amount
        FROM ds.ft_posting_f
        WHERE oper_date = i_OnDate
        GROUP BY credit_account_rk
    ),
    debet AS (
        SELECT
            debet_account_rk AS account_rk,
            SUM(debet_amount) AS debet_amount
        FROM ds.ft_posting_f
        WHERE oper_date = i_OnDate
        GROUP BY debet_account_rk
    ),
    final_data AS (
        SELECT
            i_OnDate AS on_date,
            COALESCE(c.account_rk, d.account_rk) AS account_rk,
            c.credit_amount,
            COALESCE(c.credit_amount, 0) * COALESCE(r.reduced_cource, 1) AS credit_amount_rub,
            d.debet_amount,
            COALESCE(d.debet_amount, 0) * COALESCE(r.reduced_cource, 1) AS debet_amount_rub
        FROM credit c
        FULL OUTER JOIN debet d ON c.account_rk = d.account_rk
        LEFT JOIN account_currency ac ON ac.account_rk = COALESCE(c.account_rk, d.account_rk)
        LEFT JOIN rates r ON r.currency_rk = ac.currency_rk
    )
    INSERT INTO dm.dm_account_turnover_f (
        on_date, account_rk, credit_amount, credit_amount_rub,
        debet_amount, debet_amount_rub
    )
    SELECT *
    FROM final_data;
    
    GET DIAGNOSTICS v_row_count = ROW_COUNT;

    v_end_time := clock_timestamp();
    v_duration := EXTRACT(EPOCH FROM v_end_time - v_start_time);

    INSERT INTO logs.data_load_log (
        file_name,
        load_date,
        start_time,
        end_time,
        duration_sec,
        count_row,
        error_message
    )
    VALUES (
        'dm_account_turnover_f',
        CURRENT_DATE,
        v_start_time,
        v_end_time,
        v_duration,
        v_row_count,
        NULL
    );
EXCEPTION
    WHEN OTHERS THEN
        v_end_time := clock_timestamp();
        v_duration := EXTRACT(EPOCH FROM v_end_time - v_start_time);

        INSERT INTO logs.data_load_log (
            file_name,
            load_date,
            start_time,
            end_time,
            duration_sec,
            count_row,
            error_message
        )
        VALUES (
            'dm_account_turnover_f',
            CURRENT_DATE,
            v_start_time,
            v_end_time,
            v_duration,
            0,
            SQLERRM
        );
        RAISE;
END;
$$;
