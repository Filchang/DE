INSERT INTO DM.DM_ACCOUNT_BALANCE_F (
    on_date,
    account_rk,
    balance_out,
    balance_out_rub
)
SELECT
    fb.on_date,
    fb.account_rk,
    fb.balance_out,
    fb.balance_out * COALESCE(er.reduced_cource, 1) AS balance_out_rub
FROM DS.FT_BALANCE_F fb
LEFT JOIN ds.md_exchange_rate_d er
    ON fb.currency_rk = er.currency_rk
   AND fb.on_date >= er.data_actual_date
   AND fb.on_date < er.data_actual_end_date
WHERE fb.on_date = DATE '2017-12-31';
