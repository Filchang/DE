DO $$
DECLARE
    d DATE := DATE '2018-01-01';
BEGIN
    WHILE d <= DATE '2018-01-31' LOOP
        CALL ds.fill_account_balance_f(d);
        d := d + INTERVAL '1 day';
    END LOOP;
END $$;
