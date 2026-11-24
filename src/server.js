const express = require('express');
const { Pool, DatabaseError } = require('pg');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');

// Load DB_* settings from src/.env into process.env
process.loadEnvFile(path.join(__dirname, '.env'));

const app = express();
app.use(express.json());
app.use(cors());

// Up to 20 simultaneous checkouts (the GUI's "Simulate Many Customers" limit)
// each hold one connection per pool, so they get a connection right away
// instead of queueing; queueing would also inflate the measured checkout time
const POOL_SIZE = 20;

const supermarketPool = new Pool({
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    database: process.env.DB_SUPERMARKET,
    password: process.env.DB_PASSWORD,
    port: process.env.DB_PORT,
    max: POOL_SIZE,
});

// Configure connection to the separate bank database
const bankPool = new Pool({
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    database: process.env.DB_BANK,
    password: process.env.DB_PASSWORD,
    port: process.env.DB_PORT,
    max: POOL_SIZE,
});

const POOLS = { supermarket: supermarketPool, bank: bankPool };

// Shared bank account for the entire supermarket chain
const SUPERMARKET_ACCOUNT_NUMBER = 9000;

// ------------------------------------------------------------------------------
// SQL Trace
// ------------------------------------------------------------------------------
// Every query runs through a tracer so the GUI's SQL panel can show exactly what
// each request sent and how long it took. The checkout transaction and the
// reports are also appended to the transaction.sql and query.sql trace files.
const TRACE_FILES = {
    transaction: path.join(__dirname, '..', 'transaction.sql'),
    query: path.join(__dirname, '..', 'query.sql'),
};

function createTracer(request) {
    const statements = [];
    const started = performance.now();

    return {
        // Returns an object with the same query() as the pool/client it wraps
        wrap(target, db) {
            return {
                async query(text, params) {
                    const entry = { db, sql: formatSql(text, params), ms: 0 };
                    statements.push(entry);
                    const t0 = performance.now();
                    try {
                        return await target.query(text, params);
                    } catch (error) {
                        entry.error = error.message;
                        throw error;
                    } finally {
                        entry.ms = roundMs(performance.now() - t0);
                    }
                },
            };
        },

        // Spread into a JSON response: { sql: [...], elapsedMs }
        result() {
            return { sql: statements, elapsedMs: roundMs(performance.now() - started) };
        },

        // Append this request's statements to a trace file
        save(file, outcome) {
            const header = `-- ${new Date().toISOString()}  ${request}  ` +
                `(${roundMs(performance.now() - started)} ms, ${outcome})`;
            const body = statements.map(s =>
                `-- [${s.db}] ${s.ms} ms${s.error ? `  ERROR: ${s.error}` : ''}\n` +
                (s.sql.endsWith(';') ? s.sql : `${s.sql};`));

            fs.promises.appendFile(TRACE_FILES[file], [header, ...body, '', ''].join('\n'))
                .catch(error => console.error(`Writing ${file}.sql failed: ${error.message}`));
        },
    };
}

// Readable SQL for the trace: strip the template-literal indentation and
// substitute the $n parameters with literals
function formatSql(text, params) {
    const lines = text.replace(/^\s*\n/, '').trimEnd().split('\n');
    const indent = Math.min(...lines
        .filter(line => line.trim())
        .map(line => line.match(/^ */)[0].length));
    const sql = lines.map(line => line.slice(indent)).join('\n');

    if (!params) return sql;
    return sql.replace(/\$(\d+)/g, (match, n) =>
        n - 1 < params.length ? toSqlLiteral(params[n - 1]) : match);
}

function toSqlLiteral(value) {
    if (value === null || value === undefined) return 'NULL';
    if (Array.isArray(value)) return `ARRAY[${value.map(toSqlLiteral).join(', ')}]`;
    if (typeof value === 'boolean') return value ? 'TRUE' : 'FALSE';
    if (typeof value === 'number') {
        return Number.isFinite(value) ? String(value) : `'${value}'::NUMERIC`;
    }
    if (value instanceof Date) return `'${value.toISOString()}'::TIMESTAMPTZ`;
    return `'${String(value).replace(/'/g, "''")}'`;
}

function roundMs(ms) {
    return Math.round(ms * 100) / 100;
}

// ------------------------------------------------------------------------------
// Two-Phase Commit
// ------------------------------------------------------------------------------
// A card checkout changes two databases, and a plain COMMIT on each cannot be
// atomic: whichever commits first cannot be undone if the second fails. So card
// checkouts use PostgreSQL's two-phase commit:
//
//   Phase 1: PREPARE TRANSACTION on the bank, then on the store. A prepared
//            transaction is saved to disk (it survives a crash) and keeps its
//            locks, but is not visible yet. Any failure here rolls back both.
//   Phase 2: once both are prepared the decision is commit: COMMIT PREPARED on
//            the bank, then on the store.
//
// Because the bank goes first in both phases, a crash between the steps leaves
// at most these states, each with one correct outcome (see resolveCheckout):
//
//   bank and store prepared -> both promised, so commit both
//   only store prepared     -> the bank already committed, so commit the store
//   only bank prepared      -> the store never prepared, so roll the bank back
//
// A prepared transaction keeps its row locks until it is finished, so any that
// a failure leaves behind are finished in the background (resolveLater) rather
// than waiting for a restart, and checkouts give up on a lock after
// LOCK_TIMEOUT instead of waiting forever.
//
// Requires max_prepared_transactions > 0 on the PostgreSQL server (see README).
const GID_PREFIX = 'checkout-';
// checkout-<uuid>-bank / checkout-<uuid>-supermarket
const GID_PATTERN = /^(checkout-[0-9a-f-]{36})-(bank|supermarket)$/;
// Each checkout prepares one transaction in each database, and both pools use
// the same DB_HOST/DB_PORT, so both databases share the server's
// max_prepared_transactions limit
const MIN_PREPARED_TRANSACTIONS = POOL_SIZE * 2;
// A checkout that waits longer than this for a row lock rolls back
const LOCK_TIMEOUT = '10s';
// First wait before finishing a leftover prepared transaction; doubles on each
// failure up to MAX_RETRY_MS
const RETRY_MS = 5000;
const MAX_RETRY_MS = 60000;

// One id per checkout. Transaction ids cannot be $n parameters, so they are
// generated here, never taken from a request.
function newGid() {
    return GID_PREFIX + crypto.randomUUID();
}

// Prepared transaction ids must be unique across the whole PostgreSQL server,
// and both databases can live on one server, so each database gets its own
function preparedId(gid, db) {
    return `${gid}-${db}`;
}

// True if PostgreSQL answered with an error, so the statement definitely failed.
// Otherwise the connection failed and the statement may or may not have run.
function fromServer(error) {
    return error instanceof DatabaseError;
}

// COMMIT PREPARED or ROLLBACK PREPARED. Any session can finish a prepared
// transaction, so if the checkout's own connection fails, retry once on a new one.
// Returns true if the checkout's connection failed and should be discarded.
async function finishPrepared(trace, conn, db, gid, action) {
    const sql = `${action} PREPARED '${preparedId(gid, db)}'`;
    try {
        await conn.query(sql);
        return false;
    } catch (error) {
        console.error(`${sql} on ${db} failed (${error.message}); retrying on a new connection`);
        await trace.wrap(POOLS[db], db).query(sql);
        return true;
    }
}

// Finish one checkout's leftover prepared transactions by the table above.
// Safe to repeat: a finished transaction is no longer listed.
async function resolveCheckout(trace, gid) {
    const isPrepared = async db => {
        const result = await trace.wrap(POOLS[db], db).query(`
            SELECT 1
            FROM pg_prepared_xacts
            WHERE database = current_database()
              AND gid = $1
        `, [preparedId(gid, db)]);
        return result.rows.length > 0;
    };

    const bank = await isPrepared('bank');
    const store = await isPrepared('supermarket');

    if (bank) {
        const action = store ? 'COMMIT' : 'ROLLBACK';
        await trace.wrap(bankPool, 'bank').query(`${action} PREPARED '${preparedId(gid, 'bank')}'`);
        console.log(`Resolved checkout ${gid}: bank ${action}`);
    }
    if (store) {
        await trace.wrap(supermarketPool, 'supermarket')
            .query(`COMMIT PREPARED '${preparedId(gid, 'supermarket')}'`);
        console.log(`Resolved checkout ${gid}: supermarket COMMIT`);
    }
    return bank || store;
}

// Checkouts waiting for resolveLater, so each is retried by one timer only
const pendingCheckouts = new Set();

// Finish a checkout a failure left prepared, retrying until it works. Waits
// first so that a PREPARE whose reply was lost has finished on the server
// before resolveCheckout looks for it.
function resolveLater(gid) {
    if (pendingCheckouts.has(gid)) return;
    pendingCheckouts.add(gid);

    const attempt = async delay => {
        const trace = createTracer(`resolve ${gid}`);
        try {
            if (await resolveCheckout(trace, gid)) {
                trace.save('transaction', `resolved ${gid}`);
            }
            pendingCheckouts.delete(gid);
        } catch (error) {
            const next = Math.min(delay * 2, MAX_RETRY_MS);
            console.error(`Finishing checkout ${gid} failed (${error.message}); ` +
                `retrying in ${next / 1000} s`);
            trace.save('transaction', `resolving ${gid} failed: ${error.message}`);
            setTimeout(() => attempt(next), next);
        }
    };
    setTimeout(() => attempt(RETRY_MS), RETRY_MS);
}

// Checkouts are refused until startup recovery has run, so a new checkout
// never waits on locks held by one a crash left prepared
let recovered = false;

// Finish the checkouts a crash left prepared. Each one is resolved on its own;
// any that fail are handed to resolveLater. Throws only if the prepared
// transactions could not be listed.
async function recoverCheckouts() {
    const trace = createTracer('startup recovery');

    const preparedIn = async db => {
        const result = await trace.wrap(POOLS[db], db).query(`
            SELECT gid
            FROM pg_prepared_xacts
            WHERE database = current_database()
              AND gid LIKE $1
        `, [`${GID_PREFIX}%`]);
        // Only ids this server could have made, since they go into the SQL text.
        // Returns the checkout ids (without the database suffix).
        return result.rows
            .map(r => r.gid.match(GID_PATTERN))
            .filter(match => match && match[2] === db)
            .map(match => match[1]);
    };

    const gids = new Set([...await preparedIn('bank'), ...await preparedIn('supermarket')]);

    for (const gid of gids) {
        try {
            await resolveCheckout(trace, gid);
        } catch (error) {
            console.error(`Recovering checkout ${gid} failed (${error.message}); retrying in the background`);
            resolveLater(gid);
        }
    }

    if (gids.size > 0) {
        trace.save('transaction', `recovered ${gids.size} checkouts`);
    }
}

// Checkouts fail with "prepared transactions are disabled" until the
// server allows them, so say so at startup. Checks both databases in case
// DB_BANK is ever pointed at a different server.
async function checkPreparedTransactions() {
    for (const [db, pool] of Object.entries(POOLS)) {
        try {
            const result = await pool.query('SHOW max_prepared_transactions');
            const limit = Number(result.rows[0].max_prepared_transactions);

            if (limit === 0) {
                console.warn(`WARNING: max_prepared_transactions is 0 on the ${db} database's ` +
                    'server, so checkouts (two-phase commit) will fail. See README setup.');
            } else if (limit < MIN_PREPARED_TRANSACTIONS) {
                console.warn(`WARNING: max_prepared_transactions is ${limit} on the ${db} ` +
                    `database's server; ${MIN_PREPARED_TRANSACTIONS} is needed for ` +
                    `${POOL_SIZE} simultaneous checkouts.`);
            }
        } catch (error) {
            console.error(`Checking max_prepared_transactions on ${db} failed: ${error.message}`);
        }
    }
}

// ------------------------------------------------------------------------------
// Test Transaction Endpoint
// ------------------------------------------------------------------------------
// Endpoint to test a transaction with more than 1 item
app.post('/test-transaction', async (req, res) => {
    const { storeId, items, paymentMethod, accountId } = req.body;

    // Validate storeId
    if (!Number.isInteger(storeId) || storeId <= 0) {
        return res.status(400).json({ error: 'Invalid store ID' });
    }

    // Validate Items
    if (!Array.isArray(items) || items.length === 0) {
        return res.status(400).json({ error: 'Invalid item array' });
    }

    // Sku into array
    const combinedItems = new Map();

    for (const purchasedItem of items) {
        if (!purchasedItem ||
            typeof purchasedItem.sku !== 'string' ||
            !purchasedItem.sku.trim()) {
            return res.status(400).json({ error: 'Invalid SKU' });
        }

        const quantity = purchasedItem.quantity;

        if (typeof quantity !== 'number' ||
            !Number.isFinite(quantity) ||
            quantity <= 0 ||
            Math.abs(quantity * 1000 - Math.round(quantity * 1000)) > 1e-7) {
            return res.status(400).json({ error: 'Invalid quantity' });
        }

        const sku = purchasedItem.sku.trim();

        const quantityUnits = Math.round(quantity * 1000);

        if (!Number.isSafeInteger(quantityUnits) ||
            quantityUnits <= 0) {
            return res.status(400).json({ error: 'Invalid quantity' });
        }

        const previousUnits = combinedItems.get(sku) || 0;
        const newUnits = previousUnits + quantityUnits;

        if (!Number.isSafeInteger(newUnits)) {
            return res.status(400).json({ error: 'Quantity too large' });
        }

        combinedItems.set(sku, newUnits);
    }

    // Validate payment method
    if (paymentMethod !== 'card' && paymentMethod !== 'cash') {
        return res.status(400).json({ error: 'Invalid payment method' });
    }

    if (paymentMethod === 'card' &&
        (!Number.isInteger(accountId) || accountId <= 0)) {
        return res.status(400).json({ error: 'Invalid account ID' });
    }

    if (paymentMethod === 'card' &&
        accountId === SUPERMARKET_ACCOUNT_NUMBER) {
        return res.status(400).json({
            error: 'The supermarket account cannot be used to pay.'
        });
    }
    const cart = Array.from(combinedItems, ([sku, units]) => ({
        sku,
        quantity: units / 1000
    })).sort((a, b) => a.sku < b.sku ? -1 : a.sku > b.sku ? 1 : 0);

    if (!recovered) {
        return res.status(503).json({
            success: false,
            message: 'Checkouts are unavailable until the server finishes checkouts ' +
                'left over from before it started. Try again shortly.',
        });
    }

    const trace = createTracer('POST /test-transaction');
    let client;
    let bankClient;
    let storeDb;
    let bankDb;
    let gid;
    let bankPrepared = false;
    let storePrepared = false;        // true once the decision is commit
    // A PREPARE's reply was lost, so it may or may not have happened
    let bankMaybePrepared = false;
    let storeMaybePrepared = false;
    // Connections that failed go back to the pool destroyed, not reused
    let discardStore = false;
    let discardBank = false;
    let receipt;                      // for the response if phase 2 fails
    try {
        // Connect to the store database
        client = await supermarketPool.connect();
        storeDb = trace.wrap(client, 'supermarket');
        await storeDb.query('BEGIN');
        await storeDb.query(`SET LOCAL lock_timeout = '${LOCK_TIMEOUT}'`);


        // Both card and cash payments credit the supermarket bank account
        bankClient = await bankPool.connect();
        bankDb = trace.wrap(bankClient, 'bank');
        await bankDb.query('BEGIN');
        await bankDb.query(`SET LOCAL lock_timeout = '${LOCK_TIMEOUT}'`);

        

        const validatedItems = [];
        const lineTotals = [];

        for (const purchasedItem of cart) {
            // Retrieve item information, inventory, and tax rate
            const storeResult = await storeDb.query(`
                SELECT
                    inv.price,
                    inv.quantity_in_stock,
                    i.tax_exempt,
                    i.priced_by_weight,
                    t.tax_rate
                FROM inventory inv
                JOIN item AS i ON inv.sku = i.sku
                JOIN store AS s ON inv.store_id = s.store_id
                JOIN tax AS t ON s.state_code = t.state_code
                WHERE inv.store_id = $1 AND inv.sku = $2
                FOR UPDATE OF inv
            `, [storeId, purchasedItem.sku]);

            if (storeResult.rows.length === 0) {
                throw new Error(
                    `Item ${purchasedItem.sku} not found in this store`);
            }

            const item = storeResult.rows[0];

            // Check weight based items
            if (!item.priced_by_weight &&
                !Number.isInteger(purchasedItem.quantity)) {
                throw new Error(
                    `Invalid quantity for ${purchasedItem.sku}`);
            }

            // Prevent purchases exceeding available stock
            if (Number(item.quantity_in_stock) < purchasedItem.quantity) {
                throw new Error(
                    `Not enough stock for ${purchasedItem.sku}`);
            }

            // Calculate total price including applicable sales tax
            const lineResult = await storeDb.query(`
                SELECT ROUND(
                    $1::NUMERIC * $2::NUMERIC *
                    (1 + CASE WHEN $3::BOOLEAN THEN 0 ELSE $4::NUMERIC END),
                    2
                ) AS line_total
            `, [item.price, purchasedItem.quantity, item.tax_exempt, item.tax_rate]);

            // Save calculated total
            lineTotals.push(lineResult.rows[0].line_total);

            // Save the item for the inserts below
            validatedItems.push({
                sku: purchasedItem.sku,
                quantity: purchasedItem.quantity,
                price: item.price
            });
        }

        // Add lines totals
        const totalResult = await storeDb.query(`
            SELECT COALESCE(
                SUM(amount),
                0
            ) AS total
            FROM UNNEST($1::NUMERIC[]) AS amounts(amount)
        `, [lineTotals]);

        // Get the calculated total
        const total = totalResult.rows[0].total;

        // For card payments, subtract the total from the bank balance

     
        // Only card payments subtract money from a customer account
        if (paymentMethod === 'card') {
            const bankResult = await bankDb.query(`
                UPDATE account
                SET balance = balance - $1
                WHERE account_number = $2
                  AND account_type = 'customer'
                  AND balance >= $1
                RETURNING account_number, balance
            `, [total, accountId]);

            if (bankResult.rows.length !== 1) {
                throw new Error(
                    'Insufficient funds or customer account not found'
                );
            }
        }

        // Every successful payment credits the supermarket account
        const supermarketResult = await bankDb.query(`
            UPDATE account
            SET balance = balance + $1
            WHERE account_number = $2
              AND account_type = 'supermarket'
            RETURNING account_number, balance
        `, [total, SUPERMARKET_ACCOUNT_NUMBER]);

        if (supermarketResult.rows.length !== 1) {
            throw new Error('Supermarket bank account not found');
        }



        const account = paymentMethod === 'card' ? accountId : null;

        // Create the supermarket transaction record
        const transactionResult = await storeDb.query(`
            INSERT INTO store_transaction
                (store_id, account_id, total_amount, payment_method)
            VALUES ($1, $2, $3, $4)
            RETURNING transaction_id
        `, [storeId, account, total, paymentMethod]);

        const transactionId = transactionResult.rows[0].transaction_id;

        for (const purchasedItem of validatedItems) {
            // Record the specific item purchased
            await storeDb.query(`
                INSERT INTO item_sale
                    (transaction_id, sku, quantity_sold, unit_price)
                VALUES ($1, $2, $3, $4)
            `, [transactionId, purchasedItem.sku, purchasedItem.quantity, purchasedItem.price]);

            // Subtract purchased quantity from store inventory
            await storeDb.query(`
                UPDATE inventory
                SET quantity_in_stock = quantity_in_stock - $1
                WHERE store_id = $2 AND sku = $3
            `, [purchasedItem.quantity, storeId, purchasedItem.sku]);
        }

        if (bankDb) {
            // Card: two-phase commit across both databases (see Two-Phase Commit)
            gid = newGid();
            receipt = { transactionId, total, paymentMethod };

            // Phase 1: both databases promise they can commit. If a PREPARE's
            // reply is lost, it may have happened: the catch leaves that to
            // resolveLater instead of guessing.
            try {
                await bankDb.query(`PREPARE TRANSACTION '${preparedId(gid, 'bank')}'`);
            } catch (error) {
                bankMaybePrepared = discardBank = !fromServer(error);
                throw error;
            }
            bankPrepared = true;
            try {
                await storeDb.query(`PREPARE TRANSACTION '${preparedId(gid, 'supermarket')}'`);
            } catch (error) {
                storeMaybePrepared = discardStore = !fromServer(error);
                throw error;
            }
            storePrepared = true;

            // Phase 2: commit both, bank first
            discardBank = await finishPrepared(trace, bankDb, 'bank', gid, 'COMMIT');
            discardStore = await finishPrepared(trace, storeDb, 'supermarket', gid, 'COMMIT');
        } else {
            // Cash: only the store database changes, so a plain COMMIT is atomic
            await storeDb.query('COMMIT');
        }

        console.log(`Transaction #${transactionId} at store ${storeId}: ` +
            `${validatedItems.length} items, $${total} (${paymentMethod})`);

        trace.save('transaction', `committed #${transactionId}`);

        // Return successful purchase information
        return res.json({
            ...trace.result(),
            success: true,
            message: 'Transaction successful',
            transactionId: transactionId,
            total: total,
            paymentMethod: paymentMethod,
        });
    } catch (error) {
        // Both databases promised to commit, so the checkout is committed; only
        // phase 2 failed, and resolveLater finishes it in the background
        if (storePrepared) {
            console.error(`Checkout ${gid} at store ${storeId} committed, but finishing ` +
                `it failed: ${error.message}`);
            trace.save('transaction', `committed, finish pending (${gid}): ${error.message}`);
            discardStore = discardBank = true;
            resolveLater(gid);

            return res.status(500).json({
                ...trace.result(),
                ...receipt,
                success: false,
                committed: true,
                message: `The checkout was committed, but finishing it failed ` +
                    `(${error.message}). The server will finish it in the background.`,
            });
        }

        // The bank prepared but the store's PREPARE reply was lost, so the
        // checkout may or may not be committed. The bank stays prepared, and
        // resolveLater commits both if the store did prepare, else rolls back.
        if (storeMaybePrepared) {
            console.error(`Checkout ${gid} at store ${storeId}: store PREPARE outcome ` +
                `unknown (${error.message}); resolving in the background`);
            trace.save('transaction', `outcome pending (${gid}): ${error.message}`);
            resolveLater(gid);

            return res.status(500).json({
                ...trace.result(),
                ...receipt,
                success: false,
                pending: true,
                message: `The connection failed while committing (${error.message}), so it ` +
                    `is not yet known whether the checkout went through. The server will ` +
                    `commit or roll it back as a whole within a few seconds.`,
            });
        }

        console.error(`Transaction at store ${storeId} rolled back: ${error.message}`);

        // A failed PREPARE has already rolled its transaction back, and then this
        // ROLLBACK is a harmless no-op. A failed ROLLBACK means the connection is
        // broken, and closing it rolls the transaction back.
        if (storeDb && !discardStore) {
            await storeDb.query('ROLLBACK').catch(rollbackError => {
                console.error(rollbackError);
                discardStore = true;
            });
        }
        if (bankPrepared) {
            await finishPrepared(trace, bankDb, 'bank', gid, 'ROLLBACK')
                .then(failed => { discardBank = failed; })
                .catch(rollbackError => {
                    // Still prepared and holding its locks, so keep trying
                    console.error(rollbackError);
                    discardBank = true;
                    resolveLater(gid);
                });
        } else if (bankMaybePrepared) {
            // The store did not prepare, so resolveLater rolls the bank back
            resolveLater(gid);
        } else if (bankDb) {
            await bankDb.query('ROLLBACK').catch(rollbackError => {
                console.error(rollbackError);
                discardBank = true;
            });
        }
        trace.save('transaction', `rolled back: ${error.message}`);

        return res.status(400).json({
            ...trace.result(),
            success: false,
            rolledBack: true,
            message: error.message,
        });
    } finally {
        // release(true) closes the connection instead of reusing it
        if (client) {
            client.release(discardStore);
        }
        if (bankClient) {
            bankClient.release(discardBank);
        }
    }
});

// ------------------------------------------------------------------------------
// Monthly Sales Per Store
// ------------------------------------------------------------------------------
// Retrieve monthly sales data
app.post('/reports/monthly-sales', async (req, res) => {
    const trace = createTracer('POST /reports/monthly-sales');
    const db = trace.wrap(supermarketPool, 'supermarket');

    try {
        const result = await db.query(`
            SELECT
                s.store_id,
                s.street,
                s.city,
                s.state_code,
                s.zip_code,
                DATE_TRUNC('month', t.date_and_time)::DATE
                    AS sales_month,
                COUNT(t.transaction_id)
                    AS total_transactions,
                SUM(t.total_amount)
                    AS total_revenue,
                ROUND(AVG(t.total_amount), 2)
                    AS average_transaction
            FROM store_transaction AS t
            JOIN store AS s ON t.store_id = s.store_id
            GROUP BY
                s.store_id,
                s.street,
                s.city,
                s.state_code,
                s.zip_code,
                DATE_TRUNC('month', t.date_and_time)
            ORDER BY sales_month DESC, total_revenue DESC;
        `);

        console.log(`Monthly sales report: ${result.rows.length} rows`);

        trace.save('query', `${result.rows.length} rows`);

        return res.json({
            ...trace.result(),
            success: true,
            rows: result.rows
        });
    } catch (error) {
        trace.save('query', `failed: ${error.message}`);
        console.error(`Monthly sales report failed: ${error.message}`);
        return res.status(500).json({
            ...trace.result(),
            success: false,
            message: error.message
        });
    }
});


// ------------------------------------------------------------------------------
// Top Selling Items by Department Endpoint
// ------------------------------------------------------------------------------
// Shows the highest revenue item in each department.
// By default, the report includes all stores.
// To filter by a specific store, add ?storeId=1 to the URL.

app.get('/reports/top-selling-items', async (req, res) => {

    // Filter top-selling items across all stores or within a specific store
    const storeId = req.query.storeId === undefined ? null : Number(req.query.storeId);

    // Null means result from all stores
    if (storeId !== null &&
        (!Number.isInteger(storeId) || storeId <= 0)) {

        return res.status(400).json({
            success: false,
            error: 'Invalid store ID'
        });
    }

    const trace = createTracer('GET /reports/top-selling-items');
    const db = trace.wrap(supermarketPool, 'supermarket');

    try {
        const result = await db.query(`
            WITH top_selling AS (
                SELECT
                    d.department_id,
                    d.name AS department_name,
                    i.sku,
                    i.name AS item_name,
                    SUM(s.quantity_sold) AS total_quantity_sold,
                    SUM(s.quantity_sold * s.unit_price) AS total_revenue
                FROM item_sale AS s
                JOIN item AS i ON s.sku = i.sku
                JOIN department AS d ON i.department_id = d.department_id
                JOIN store_transaction AS t ON s.transaction_id = t.transaction_id
                WHERE ($1::INTEGER IS NULL OR t.store_id = $1)
                GROUP BY d.department_id, d.name, i.sku, i.name
        ),
        ranked_products AS (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY department_id 
                ORDER BY total_revenue DESC, sku
                ) AS product_rank
            FROM top_selling
        )
        SELECT 
            department_name, 
            sku, 
            item_name, 
            total_quantity_sold, 
            total_revenue
        FROM ranked_products
        WHERE product_rank = 1
        ORDER BY department_name;
        `, [storeId]);

        trace.save('query', `${result.rows.length} rows`);

        return res.json({
            ...trace.result(),
            success: true,
            storeId: storeId,
            items: result.rows
        });

    } catch (error) {
        trace.save('query', `failed: ${error.message}`);
        return res.status(500).json({
            ...trace.result(),
            success: false,
            message: error.message
        });
    }
});

// ------------------------------------------------------------------------------
// Low Stock Reorder List
// ------------------------------------------------------------------------------
// Items below the stock threshold, with how fast they sold over the last 30 days
// so the fastest-selling shortages come first. Optional storeId limits it to one
// store; threshold defaults to 10 (units, or lbs for weighed items).
app.post('/reports/low-stock', async (req, res) => {
    const { storeId = null, threshold = 10 } = req.body;

    if (storeId !== null &&
        (!Number.isInteger(storeId) || storeId <= 0)) {
        return res.status(400).json({ success: false, message: 'Invalid store ID' });
    }

    if (typeof threshold !== 'number' ||
        !Number.isFinite(threshold) ||
        threshold <= 0) {
        return res.status(400).json({ success: false, message: 'Invalid threshold' });
    }

    const trace = createTracer('POST /reports/low-stock');
    const db = trace.wrap(supermarketPool, 'supermarket');

    try {
        const result = await db.query(`
            SELECT
                s.store_id,
                s.street,
                s.city,
                s.state_code,
                inv.aisle_no,
                i.sku,
                i.name AS item_name,
                d.name AS department_name,
                i.priced_by_weight,
                inv.quantity_in_stock,
                COALESCE(recent.quantity_sold, 0) AS sold_last_30_days
            FROM inventory AS inv
            JOIN item AS i ON inv.sku = i.sku
            JOIN department AS d ON i.department_id = d.department_id
            JOIN store AS s ON inv.store_id = s.store_id
            LEFT JOIN (
                SELECT
                    t.store_id,
                    si.sku,
                    SUM(si.quantity_sold) AS quantity_sold
                FROM item_sale AS si
                JOIN store_transaction AS t ON si.transaction_id = t.transaction_id
                WHERE t.date_and_time >= now() - INTERVAL '30 days'
                GROUP BY t.store_id, si.sku
            ) AS recent ON recent.store_id = inv.store_id AND recent.sku = inv.sku
            WHERE inv.quantity_in_stock < $1
              AND ($2::INTEGER IS NULL OR inv.store_id = $2)
            ORDER BY
                inv.quantity_in_stock,
                sold_last_30_days DESC,
                s.store_id,
                inv.aisle_no;
        `, [threshold, storeId]);

        console.log(`Low stock report (store ${storeId ?? 'all'}, < ${threshold}): ` +
            `${result.rows.length} rows`);

        trace.save('query', `${result.rows.length} rows`);

        return res.json({
            ...trace.result(),
            success: true,
            rows: result.rows
        });
    } catch (error) {
        trace.save('query', `failed: ${error.message}`);
        console.error(`Low stock report failed: ${error.message}`);
        return res.status(500).json({
            ...trace.result(),
            success: false,
            message: error.message
        });
    }
});


// ------------------------------------------------------------------------------
// Recent Transactions
// ------------------------------------------------------------------------------
// Checkouts recorded in the last `minutes`, newest first, so the data written by
// a simulation run can be viewed right after it. Optional storeId limits it to
// one store.
const RECENT_TRANSACTIONS_LIMIT = 500;

app.post('/reports/recent-transactions', async (req, res) => {
    const { minutes = 60, storeId = null } = req.body;

    // Up to one year back
    if (!Number.isInteger(minutes) || minutes <= 0 || minutes > 525600) {
        return res.status(400).json({ success: false, message: 'Invalid time window' });
    }

    if (storeId !== null &&
        (!Number.isInteger(storeId) || storeId <= 0)) {
        return res.status(400).json({ success: false, message: 'Invalid store ID' });
    }

    const trace = createTracer('POST /reports/recent-transactions');
    const db = trace.wrap(supermarketPool, 'supermarket');

    try {
        // Grouping by the primary keys lets every other column of t and s be selected
        const result = await db.query(`
            SELECT
                t.transaction_id,
                t.date_and_time,
                s.store_id,
                s.street,
                s.city,
                s.state_code,
                t.payment_method,
                t.account_id,
                COUNT(si.sku) AS item_count,
                t.total_amount
            FROM store_transaction AS t
            JOIN store AS s ON t.store_id = s.store_id
            LEFT JOIN item_sale AS si ON si.transaction_id = t.transaction_id
            WHERE t.date_and_time >= now() - make_interval(mins => $1)
              AND ($2::INTEGER IS NULL OR t.store_id = $2)
            GROUP BY t.transaction_id, s.store_id
            ORDER BY t.date_and_time DESC
            LIMIT $3;
        `, [minutes, storeId, RECENT_TRANSACTIONS_LIMIT]);

        console.log(`Recent transactions (last ${minutes} min, store ${storeId ?? 'all'}): ` +
            `${result.rows.length} rows`);

        trace.save('query', `${result.rows.length} rows`);

        return res.json({
            ...trace.result(),
            success: true,
            rows: result.rows
        });
    } catch (error) {
        trace.save('query', `failed: ${error.message}`);
        console.error(`Recent transactions report failed: ${error.message}`);
        return res.status(500).json({
            ...trace.result(),
            success: false,
            message: error.message
        });
    }
});

// ------------------------------------------------------------------------------
// Table Browser
// ------------------------------------------------------------------------------
// The tables the GUI can browse and empty. Table names cannot be SQL parameters,
// so only names from this list are ever put into the SQL text.
const TABLES = {
    tax: { db: 'supermarket', orderBy: 'state_code' },
    department: { db: 'supermarket', orderBy: 'department_id' },
    store: { db: 'supermarket', orderBy: 'store_id' },
    item: { db: 'supermarket', orderBy: 'sku' },
    inventory: { db: 'supermarket', orderBy: 'store_id, sku' },
    store_transaction: { db: 'supermarket', orderBy: 'transaction_id DESC' },
    item_sale: { db: 'supermarket', orderBy: 'transaction_id DESC, sku' },
    account: { db: 'bank', orderBy: 'account_number' },
};


const BROWSE_LIMIT = 10;

function lookupTable(name) {
    return Object.hasOwn(TABLES, name) ? TABLES[name] : null;
}

// Every table with its row count (null if it does not exist yet)
app.post('/tables', async (req, res) => {
    const trace = createTracer('POST /tables');

    try {
        const tables = [];

        for (const [db, pool] of Object.entries(POOLS)) {
            const names = Object.keys(TABLES).filter(name => TABLES[name].db === db);

            // to_regclass is NULL for a missing table, so this works before a reset
            const existing = await trace.wrap(pool, db).query(`
                SELECT name
                FROM UNNEST($1::TEXT[]) AS t(name)
                WHERE to_regclass(name) IS NOT NULL
            `, [names]);
            const present = existing.rows.map(r => r.name);

            const counts = new Map();
            if (present.length > 0) {
                const result = await trace.wrap(pool, db).query(
                    present.map(name =>
                        `SELECT '${name}' AS name, count(*)::int AS row_count FROM ${name}`
                    ).join('\nUNION ALL\n'));
                result.rows.forEach(r => counts.set(r.name, r.row_count));
            }

            names.forEach(name => tables.push({ name, db, rowCount: counts.get(name) ?? null }));
        }

        res.json({ ...trace.result(), success: true, tables });
    } catch (error) {
        console.error(`Listing tables failed: ${error.message}`);
        res.status(500).json({ ...trace.result(), success: false, message: error.message });
    }
});

// The first few rows of one table
app.post('/tables/browse', async (req, res) => {
    const { table } = req.body;
    const config = lookupTable(table);

    if (!config) {
        return res.status(400).json({ success: false, message: 'Unknown table' });
    }

    const trace = createTracer('POST /tables/browse');
    const db = trace.wrap(POOLS[config.db], config.db);

    try {
        const result = await db.query(`
            SELECT *
            FROM ${table}
            ORDER BY ${config.orderBy}
            LIMIT $1;
        `, [BROWSE_LIMIT]);

        res.json({ ...trace.result(), success: true, table, rows: result.rows });
    } catch (error) {
        console.error(`Browsing ${table} failed: ${error.message}`);
        res.status(500).json({ ...trace.result(), success: false, message: error.message });
    }
});

// Delete every row of one table, in a transaction. Rows other tables still
// reference make it fail and roll back, so children must be emptied first.
app.post('/tables/clear', async (req, res) => {
    const { table } = req.body;
    const config = lookupTable(table);

    if (!config) {
        return res.status(400).json({ success: false, message: 'Unknown table' });
    }

    const trace = createTracer('POST /tables/clear');
    let client;
    let db;

    try {
        client = await POOLS[config.db].connect();
        db = trace.wrap(client, config.db);

        await db.query('BEGIN');
        const result = await db.query(`DELETE FROM ${table};`);
        await db.query('COMMIT');

        console.log(`Deleted all ${result.rowCount} rows from ${table}`);
        res.json({ ...trace.result(), success: true, table, deleted: result.rowCount });
    } catch (error) {
        if (db) {
            await db.query('ROLLBACK').catch(console.error);
        }

        // 23503 = foreign_key_violation; the detail names the referencing table
        const message = error.code === '23503'
            ? `Rows in ${table} are still referenced by another table ` +
            `(${(error.detail ?? error.message).replace(/\.$/, '')}). ` +
            `Delete that table's rows first.`
            : error.message;

        console.error(`Deleting rows from ${table} rolled back: ${error.message}`);
        res.status(error.code === '23503' ? 409 : 500)
            .json({ ...trace.result(), success: false, message });
    } finally {
        if (client) {
            client.release();
        }
    }
});

// ------------------------------------------------------------------------------
// Reset Endpoint
// ------------------------------------------------------------------------------
app.post('/reset-tables', async (req, res) => {
    const trace = createTracer('POST /reset-tables');
    const db = trace.wrap(supermarketPool, 'supermarket');

    try {
        // Reset the supermarket database
        const ddl = fs.readFileSync(path.join(__dirname, '..', 'ddl', 'ddl.sql'), 'utf8');
        await db.query(ddl);
        console.log('Supermarket tables reset from ddl.sql');

        res.json({ ...trace.result(), success: true });
    } catch (error) {
        console.error(`Supermarket reset failed: ${error.message}`);
        res.status(500).json({ ...trace.result(), success: false, message: error.message });
    }
});

// ------------------------------------------------------------------------------
// Bank Accounts Endpoint
// ------------------------------------------------------------------------------
app.post('/accounts', async (req, res) => {
    const trace = createTracer('POST /accounts');
    const db = trace.wrap(bankPool, 'bank');

    try {
        const queryAccounts = `
            SELECT account_number, balance
            FROM account
            ORDER BY account_number;
        `;
        const accounts = await db.query(queryAccounts);
        console.log(`Loaded ${accounts.rows.length} bank accounts`);

        res.json({
            ...trace.result(),
            success: true,
            accounts: accounts.rows,
        });
    } catch (error) {
        console.error(`Loading bank accounts failed: ${error.message}`);
        res.status(500).json({ ...trace.result(), success: false, message: error.message });
    }
});

// ------------------------------------------------------------------------------
// Stores Endpoint
// ------------------------------------------------------------------------------
app.post('/stores', async (req, res) => {
    const trace = createTracer('POST /stores');
    const db = trace.wrap(supermarketPool, 'supermarket');

    try {
        // Get the stores table
        const queryStores = `
            SELECT *
            FROM store
            ORDER BY store_id;
        `;
        const stores = await db.query(queryStores);
        console.log(`Loaded ${stores.rows.length} stores`);

        res.json({
            ...trace.result(),
            success: true,
            stores: stores.rows,
        });
    } catch (error) {
        console.error(`Loading stores failed: ${error.message}`);
        res.status(500).json({ ...trace.result(), success: false, message: error.message });
    }
});

// ------------------------------------------------------------------------------
// Bank Reset Endpoint
// ------------------------------------------------------------------------------
app.post('/reset-bank', async (req, res) => {
    const trace = createTracer('POST /reset-bank');
    const db = trace.wrap(bankPool, 'bank');

    try {
        // Reset the bank database
        const ddl = fs.readFileSync(path.join(__dirname, '..', 'ddl', 'bankddl.sql'), 'utf8');
        await db.query(ddl);

        const queryAccounts = `SELECT count(*)::int AS count FROM account;`;
        const accounts = await db.query(queryAccounts);
        console.log(`Bank tables reset from bankddl.sql: ${accounts.rows[0].count} accounts`);

        res.json({
            ...trace.result(),
            success: true,
            accountCount: accounts.rows[0].count,
        });
    } catch (error) {
        console.error(`Bank reset failed: ${error.message}`);
        res.status(500).json({ ...trace.result(), success: false, message: error.message });
    }
});

// ------------------------------------------------------------------------------
// Inventory Endpoint
// ------------------------------------------------------------------------------
app.post('/inventory', async (req, res) => {
    const { storeID } = req.body;

    const trace = createTracer('POST /inventory');
    const db = trace.wrap(supermarketPool, 'supermarket');

    try {
        const queryInventory = `
            SELECT *
            FROM inventory
            JOIN item ON inventory.sku = item.sku
            WHERE store_id = $1;
        `;
        const items = await db.query(queryInventory, [storeID]);
        console.log(`Loaded ${items.rows.length} inventory items for store ${storeID}`);

        res.json({
            ...trace.result(),
            success: true,
            items: items.rows,
        });
    } catch (error) {
        console.error(`Loading inventory for store ${storeID} failed: ${error.message}`);
        res.status(500).json({ ...trace.result(), success: false, message: error.message });
    }
});

// ------------------------------------------------------------------------------
// Start DB
// ------------------------------------------------------------------------------
// Finish any checkouts a crash left prepared before accepting checkouts.
// If the databases are unreachable, start anyway so the GUI can report the
// errors, and keep retrying recovery; checkouts are refused until it works.
async function recoverUntilDone(delay = RETRY_MS) {
    try {
        await recoverCheckouts();
        recovered = true;
    } catch (error) {
        const next = Math.min(delay * 2, MAX_RETRY_MS);
        console.error(`Recovering prepared checkouts failed (${error.message}); card ` +
            `checkouts are disabled until it succeeds. Retrying in ${delay / 1000} s`);
        setTimeout(() => recoverUntilDone(next), delay);
    }
}

async function start() {
    await checkPreparedTransactions();
    await recoverUntilDone();

    app.listen(3000, () => console.log(`Supermarket API running on http://localhost:3000`));
}

start();
