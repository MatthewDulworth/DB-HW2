-- drop tables to allow clean re-runs
DROP TABLE IF EXISTS account;

------------ Account ------------
CREATE TABLE account (
    account_number INTEGER PRIMARY KEY,
    balance NUMERIC(12,2) NOT NULL,
    account_type VARCHAR(11) NOT NULL DEFAULT 'customer',

    CONSTRAINT nonnegative_balance CHECK (balance >= 0),

    CONSTRAINT valid_account_type
        CHECK (account_type IN ('customer', 'supermarket')),

    CONSTRAINT reserved_supermarket_account
        CHECK (
            (account_number = 9000) =
            (account_type = 'supermarket')
        )
);

------------------------------------------------------------
------------ Random test data ------------
------------------------------------------------------------
-- fixed seed so re-runs produce the same "random" data
SELECT setseed(0.42);

------------ Account ------------
-- 300 accounts numbered 1000-1299 with balances from $0 to $5,000;
-- every 10th account starts nearly empty to exercise insufficient funds
INSERT INTO account (account_number, balance)
SELECT
    n,
    CASE WHEN n % 10 = 0
         THEN round((random() * 20)::numeric, 2)
         ELSE round((50 + random() * 4950)::numeric, 2) END
FROM generate_series(1000, 1299) AS n;

------------ Supermarket Account ------------
INSERT INTO account (
    account_number,
    balance,
    account_type
)
VALUES (
    9000,
    0.00,
    'supermarket'
);