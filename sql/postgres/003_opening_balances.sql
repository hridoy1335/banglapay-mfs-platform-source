
-- ==========================================================
-- BANGLAPAY
-- PHASE 3: CHART OF ACCOUNTS AND OPENING BALANCES
--
-- IMPORTANT:
-- Execute ONCE on the initialized BanglaPay database.
-- This is a synthetic opening financial position.
-- ==========================================================

BEGIN;


-- ----------------------------------------------------------
-- 1. SAFETY CHECKS
-- ----------------------------------------------------------

DO $$
BEGIN

    IF EXISTS (
        SELECT 1
        FROM accounting.journal_entries
        WHERE journal_ref = 'OPENING-INITIAL-001'
    ) THEN
        RAISE EXCEPTION
            'Opening journal already exists. Do not rerun this script.';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM wallet.wallet_balances
        WHERE book_balance <> 0
           OR available_balance <> 0
           OR held_balance <> 0
    ) THEN
        RAISE EXCEPTION
            'Wallet balances are not zero. Opening initialization aborted.';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM network.agent_float_accounts
        WHERE electronic_float_balance <> 0
           OR reserved_electronic_float <> 0
        ) THEN
        RAISE EXCEPTION
            'Agent float is not zero. Opening initialization aborted.';
    END IF;

END $$;


-- ----------------------------------------------------------
-- 2. SYSTEM CHART OF ACCOUNTS
-- ----------------------------------------------------------

INSERT INTO accounting.ledger_accounts (
    account_code,
    account_name,
    account_type,
    normal_balance,
    owner_type,
    wallet_id,
    agent_float_account_id,
    merchant_id,
    currency,
    is_active
)
VALUES
(
    'SYS-TRUST-SETTLEMENT-ASSET',
    'Trust and Settlement Bank Asset',
    'ASSET',
    'DEBIT',
    'SYSTEM',
    NULL, NULL, NULL, 'BDT', TRUE
),
(
    'SYS-BANK-SETTLEMENT-ASSET',
    'Bank Settlement Asset',
    'ASSET',
    'DEBIT',
    'SYSTEM',
    NULL, NULL, NULL, 'BDT', TRUE
),
(
    'SYS-CASH-IN-TRANSIT-ASSET',
    'Cash In Transit Asset',
    'ASSET',
    'DEBIT',
    'SYSTEM',
    NULL, NULL, NULL, 'BDT', TRUE
),
(
    'SYS-FEE-REVENUE',
    'Transaction Fee Revenue',
    'REVENUE',
    'CREDIT',
    'SYSTEM',
    NULL, NULL, NULL, 'BDT', TRUE
),
(
    'SYS-OPERATING-EXPENSE',
    'Operating Expense',
    'EXPENSE',
    'DEBIT',
    'SYSTEM',
    NULL, NULL, NULL, 'BDT', TRUE
)
ON CONFLICT DO NOTHING;


-- ----------------------------------------------------------
-- 3. CUSTOMER WALLET LEDGER ACCOUNTS
-- ----------------------------------------------------------

INSERT INTO accounting.ledger_accounts (
    account_code,
    account_name,
    account_type,
    normal_balance,
    owner_type,
    wallet_id,
    agent_float_account_id,
    merchant_id,
    currency,
    is_active
)
SELECT
    'WAL-' || w.wallet_ref,
    'Customer Wallet Liability - ' || w.wallet_ref,
    'LIABILITY',
    'CREDIT',
    'WALLET',
    w.wallet_id,
    NULL,
    NULL,
    'BDT',
    (w.wallet_status = 'ACTIVE')
FROM wallet.wallets w

ON CONFLICT DO NOTHING;


-- ----------------------------------------------------------
-- 4. AGENT ELECTRONIC FLOAT LEDGER ACCOUNTS
-- ----------------------------------------------------------

INSERT INTO accounting.ledger_accounts (
    account_code,
    account_name,
    account_type,
    normal_balance,
    owner_type,
    wallet_id,
    agent_float_account_id,
    merchant_id,
    currency,
    is_active
)
SELECT
    'AGF-' || f.float_account_ref,
    'Agent Electronic Float Liability - ' || f.float_account_ref,
    'LIABILITY',
    'CREDIT',
    'AGENT_FLOAT',
    NULL,
    f.float_account_id,
    NULL,
    'BDT',
    (o.outlet_status = 'ACTIVE')
FROM network.agent_float_accounts f
JOIN network.agent_outlets o
    ON o.outlet_id = f.outlet_id

ON CONFLICT DO NOTHING;


-- ----------------------------------------------------------
-- 5. MERCHANT PAYABLE LEDGER ACCOUNTS
-- ----------------------------------------------------------

INSERT INTO accounting.ledger_accounts (
    account_code,
    account_name,
    account_type,
    normal_balance,
    owner_type,
    wallet_id,
    agent_float_account_id,
    merchant_id,
    currency,
    is_active
)
SELECT
    'MRP-' || m.merchant_ref,
    'Merchant Payable Liability - ' || m.merchant_ref,
    'LIABILITY',
    'CREDIT',
    'MERCHANT',
    NULL,
    NULL,
    m.merchant_id,
    'BDT',
    (m.merchant_status = 'ACTIVE')
FROM merchant.merchants m

ON CONFLICT DO NOTHING;


-- ----------------------------------------------------------
-- 6. CREATE DETERMINISTIC OPENING POSTINGS
--
-- Wallet range:
-- BDT 500 - 10,000
--
-- Agent electronic float:
-- BDT 50,000 - 200,000
--
-- Agent physical cash:
-- BDT 30,000 - 150,000
--
-- Hash-based generation keeps the same amount on retries.
-- All amounts are generated in integer paisa first.
-- ----------------------------------------------------------

CREATE TEMP TABLE bp_opening_postings (

    account_id UUID NOT NULL,

    wallet_id UUID,

    float_account_id UUID,

    amount NUMERIC(18,2) NOT NULL,

    physical_cash_amount NUMERIC(18,2),

    description TEXT NOT NULL

) ON COMMIT DROP;


-- Active customer wallet opening balances

INSERT INTO pg_temp.bp_opening_postings (
    account_id,
    wallet_id,
    float_account_id,
    amount,
    physical_cash_amount,
    description
)
SELECT
    la.account_id,
    w.wallet_id,
    NULL,

    (
        50000
        +
        (
            (
                ('x' || SUBSTR(MD5(w.wallet_ref), 1, 8))
                ::bit(32)::bigint
            ) % 950001
        )
    )::numeric / 100,

    NULL,

    'Opening Customer Wallet Balance: ' || w.wallet_ref

FROM wallet.wallets w

JOIN accounting.ledger_accounts la
    ON la.wallet_id = w.wallet_id
   AND la.owner_type = 'WALLET'

WHERE w.wallet_status = 'ACTIVE';


-- Active agent outlet electronic float and physical cash

INSERT INTO pg_temp.bp_opening_postings (
    account_id,
    wallet_id,
    float_account_id,
    amount,
    physical_cash_amount,
    description
)
SELECT
    la.account_id,
    NULL,
    f.float_account_id,

    (
        5000000
        +
        (
            (
                ('x' || SUBSTR(MD5(f.float_account_ref), 1, 8))
                ::bit(32)::bigint
            ) % 15000001
        )
    )::numeric / 100,

    (
        3000000
        +
        (
            (
                ('x' || SUBSTR(MD5(f.float_account_ref), 1, 8))
                ::bit(32)::bigint
            ) % 12000001
        )
    )::numeric / 100,

    'Opening Agent Electronic Float: ' || f.float_account_ref

FROM network.agent_float_accounts f

JOIN network.agent_outlets o
    ON o.outlet_id = f.outlet_id

JOIN accounting.ledger_accounts la
    ON la.agent_float_account_id = f.float_account_id
   AND la.owner_type = 'AGENT_FLOAT'

WHERE o.outlet_status = 'ACTIVE';


-- ----------------------------------------------------------
-- 7. VALIDATE OPENING ACCOUNT COUNTS
-- ----------------------------------------------------------

DO $$
DECLARE
    active_wallet_count INTEGER;
    wallet_posting_count INTEGER;

    active_outlet_count INTEGER;
    agent_posting_count INTEGER;

BEGIN

    SELECT COUNT(*)
    INTO active_wallet_count
    FROM wallet.wallets
    WHERE wallet_status = 'ACTIVE';

    SELECT COUNT(*)
    INTO wallet_posting_count
    FROM pg_temp.bp_opening_postings
    WHERE wallet_id IS NOT NULL;

    SELECT COUNT(*)
    INTO active_outlet_count
    FROM network.agent_outlets
    WHERE outlet_status = 'ACTIVE';

    SELECT COUNT(*)
    INTO agent_posting_count
    FROM pg_temp.bp_opening_postings
    WHERE float_account_id IS NOT NULL;

    IF active_wallet_count <> wallet_posting_count THEN
        RAISE EXCEPTION
            'Wallet posting count mismatch: expected %, generated %',
            active_wallet_count, wallet_posting_count;
    END IF;

    IF active_outlet_count <> agent_posting_count THEN
        RAISE EXCEPTION
            'Agent float posting count mismatch: expected %, generated %',
            active_outlet_count, agent_posting_count;
    END IF;

    IF wallet_posting_count = 0 OR agent_posting_count = 0 THEN
        RAISE EXCEPTION
            'Opening postings cannot be empty.';
    END IF;

END $$;


-- ----------------------------------------------------------
-- 8. UPDATE WALLET BALANCES
-- ----------------------------------------------------------

UPDATE wallet.wallet_balances b

SET
    book_balance = p.amount,
    available_balance = p.amount,
    held_balance = 0,
    balance_version = 1,
    updated_at = NOW()

FROM pg_temp.bp_opening_postings p

WHERE p.wallet_id = b.wallet_id;


-- ----------------------------------------------------------
-- 9. UPDATE AGENT FLOAT ACCOUNTS
-- ----------------------------------------------------------

UPDATE network.agent_float_accounts f

SET
    electronic_float_balance = p.amount,
    reserved_electronic_float = 0,
    physical_cash_balance = p.physical_cash_amount,
    last_reconciled_at = NOW(),
    updated_at = NOW()

FROM pg_temp.bp_opening_postings p

WHERE p.float_account_id = f.float_account_id;


-- ----------------------------------------------------------
-- 10. CREATE BALANCED OPENING JOURNAL
-- ----------------------------------------------------------

WITH new_journal AS (

    INSERT INTO accounting.journal_entries (
        journal_ref,
        journal_type,
        journal_status,
        currency,
        effective_at,
        posted_at
    )

    VALUES (
        'OPENING-INITIAL-001',
        'ADJUSTMENT',
        'POSTED',
        'BDT',
        NOW(),
        NOW()
    )

    RETURNING journal_id
),

opening_total AS (

    SELECT
        SUM(amount)::numeric(18,2) AS total_amount

    FROM pg_temp.bp_opening_postings

),

debit_line AS (

    SELECT
        j.journal_id,
        a.account_id,
        'BDT'::char(3) AS currency,
        1::smallint AS line_number,
        'DEBIT'::varchar(10) AS entry_side,
        t.total_amount AS amount,
        'Synthetic opening backing asset'::varchar(200)
            AS description

    FROM new_journal j

    CROSS JOIN opening_total t

    CROSS JOIN accounting.ledger_accounts a

    WHERE a.account_code = 'SYS-TRUST-SETTLEMENT-ASSET'

),

credit_lines AS (

    SELECT
        j.journal_id,
        p.account_id,
        'BDT'::char(3) AS currency,

        (
            ROW_NUMBER() OVER (
                ORDER BY p.description, p.account_id
            ) + 1
        )::smallint AS line_number,

        'CREDIT'::varchar(10) AS entry_side,

        p.amount,

        p.description::varchar(200) AS description

    FROM new_journal j

    CROSS JOIN pg_temp.bp_opening_postings p

),

all_lines AS (

    SELECT * FROM debit_line

    UNION ALL

    SELECT * FROM credit_lines

)

INSERT INTO accounting.ledger_entries (
    journal_id,
    account_id,
    currency,
    line_number,
    entry_side,
    amount,
    description
)

SELECT
    journal_id,
    account_id,
    currency,
    line_number,
    entry_side,
    amount,
    description

FROM all_lines;


-- ----------------------------------------------------------
-- 11. FINAL DEBIT / CREDIT BALANCE CHECK
-- ----------------------------------------------------------

DO $$
DECLARE
    debit_total NUMERIC(18,2);
    credit_total NUMERIC(18,2);

BEGIN

    SELECT
        COALESCE(
            SUM(amount) FILTER (WHERE entry_side = 'DEBIT'),
            0
        ),

        COALESCE(
            SUM(amount) FILTER (WHERE entry_side = 'CREDIT'),
            0
        )

    INTO debit_total, credit_total

    FROM accounting.ledger_entries le

    JOIN accounting.journal_entries je
        ON je.journal_id = le.journal_id

    WHERE je.journal_ref = 'OPENING-INITIAL-001';


    IF debit_total <> credit_total THEN
        RAISE EXCEPTION
            'Opening journal is unbalanced. Debit: %, Credit: %',
            debit_total, credit_total;
    END IF;

    RAISE NOTICE
        'Opening journal balanced. Debit: %, Credit: %',
        debit_total, credit_total;

END $$;


COMMIT;
