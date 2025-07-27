CREATE OR REPLACE PROCEDURE ds.fill_account_balance_f(i_OnDate date)
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP := clock_timestamp();
    v_end_time TIMESTAMP;
    v_duration DOUBLE PRECISION;
    v_row_count INTEGER := 0;
BEGIN
    -- Удалим старые данные
    DELETE FROM dm.dm_account_balance_f
    WHERE on_date = i_OnDate;

    WITH
    -- Остатки на предыдущую дату
    prev_balance AS (
        SELECT account_rk, balance_out, balance_out_rub
        FROM dm.dm_account_balance_f
        WHERE on_date = i_OnDate - INTERVAL '1 day'
    ),
    
    -- Обороты за i_OnDate
    turnovers AS (
        SELECT 
            account_rk, 
            debet_amount, 
            credit_amount,
            debet_amount_rub,
            credit_amount_rub
        FROM dm.dm_account_turnover_f
        WHERE on_date = i_OnDate
    ),

    -- Валюта счета
    account_currency AS (
        SELECT DISTINCT account_rk, currency_rk
        FROM ds.ft_balance_f
        WHERE on_date = i_OnDate
    ),

    -- Курсы валют
    rates AS (
        SELECT currency_rk, reduced_cource
        FROM ds.md_exchange_rate_d
        WHERE i_OnDate >= data_actual_date
          AND i_OnDate < data_actual_end_date
    ),

    -- Объединенные данные
    -- Объединенные данные
	data AS (
	    SELECT
	        acc.account_rk,
	        acc.char_type,
	        COALESCE(pb.balance_out, 0) AS prev_balance,
	        COALESCE(pb.balance_out_rub, 0) AS prev_balance_rub,
	        COALESCE(t.debet_amount, 0) AS debet,
	        COALESCE(t.credit_amount, 0) AS credit,
	        COALESCE(t.debet_amount_rub, 0) AS debet_rub,
	        COALESCE(t.credit_amount_rub, 0) AS credit_rub,
	        COALESCE(r.reduced_cource, 1) AS rate
	    FROM ds.md_account_d acc
	    LEFT JOIN prev_balance pb ON pb.account_rk = acc.account_rk
	    LEFT JOIN turnovers t ON t.account_rk = acc.account_rk
	    LEFT JOIN account_currency ac ON ac.account_rk = acc.account_rk
	    LEFT JOIN rates r ON r.currency_rk = ac.currency_rk
	    WHERE i_OnDate BETWEEN acc.data_actual_date AND acc.data_actual_end_date
	)

    -- Вставка остатков
    INSERT INTO dm.dm_account_balance_f (
        on_date,
        account_rk,
        balance_out,
        balance_out_rub
    )
    SELECT
        i_OnDate,
        account_rk,
        CASE char_type
            WHEN 'А' THEN prev_balance + debet - credit
            WHEN 'П' THEN prev_balance - debet + credit
        END AS balance_out,
        CASE char_type
            WHEN 'А' THEN (prev_balance + debet - credit) * rate
            WHEN 'П' THEN (prev_balance - debet + credit) * rate
        END AS balance_out_rub
    FROM data;

    GET DIAGNOSTICS v_row_count = ROW_COUNT;

    v_end_time := clock_timestamp();
    v_duration := EXTRACT(EPOCH FROM v_end_time - v_start_time);

    -- Лог успешной загрузки
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
        'dm_account_balance_f',
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
            'dm_account_balance_f',
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
