const express = require('express');
const { Pool } = require('pg');
const cors = require('cors');
const path = require('path');
const fs = require('fs');

// Load DB_* settings from src/.env into process.env
process.loadEnvFile(path.join(__dirname, '.env'));

const app = express();
app.use(express.json());
app.use(cors());

const supermarketPool = new Pool({
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    database: process.env.DB_SUPERMARKET,
    password: process.env.DB_PASSWORD,
    port: process.env.DB_PORT,
});

// Configure connection to the separate bank database
const bankPool = new Pool({
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    database: process.env.DB_BANK,
    password: process.env.DB_PASSWORD,
    port: process.env.DB_PORT,
});

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
    if (typeof value === 'number') return String(value);
    return `'${String(value).replace(/'/g, "''")}'`;
}

function roundMs(ms) {
    return Math.round(ms * 100) / 100;
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

    const cart = Array.from(combinedItems, ([sku, units]) => ({
        sku,
        quantity: units / 1000
    })).sort((a, b) => a.sku < b.sku ? -1 : a.sku > b.sku ? 1 : 0);

    const trace = createTracer('POST /test-transaction');
    let client;
    let bankClient;
    let storeDb;
    let bankDb;
    try {
        // Connect to the store database
        client = await supermarketPool.connect();
        storeDb = trace.wrap(client, 'supermarket');
        await storeDb.query('BEGIN');

        // Connect to the bank database for card payments
        if (paymentMethod === 'card') {
            bankClient = await bankPool.connect();
            bankDb = trace.wrap(bankClient, 'bank');
            await bankDb.query('BEGIN');
        }

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
        if (paymentMethod === 'card') {
            const bankResult = await bankDb.query(`
                UPDATE account
                SET balance = balance - $1
                WHERE account_number = $2
                  AND balance >= $1
                RETURNING account_number, balance
            `, [total, accountId]);

            if (bankResult.rows.length === 0) {
                throw new Error('Insufficient funds or account not found');
            }
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
            await bankDb.query('COMMIT');
        }
        await storeDb.query('COMMIT');

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
        console.error(`Transaction at store ${storeId} rolled back: ${error.message}`);

        if (storeDb) {
            await storeDb.query('ROLLBACK').catch(console.error);
        }
        if (bankDb) {
            await bankDb.query('ROLLBACK').catch(console.error);
        }
        trace.save('transaction', `rolled back: ${error.message}`);

        return res.status(400).json({
            ...trace.result(),
            success: false,
            message: error.message,
        });
    } finally {
        if (client) {
            client.release();
        }
        if (bankClient) {
            bankClient.release();
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
            JOIN item_sale AS si ON si.transaction_id = t.transaction_id
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
    tax:               { db: 'supermarket', orderBy: 'state_code' },
    department:        { db: 'supermarket', orderBy: 'department_id' },
    store:             { db: 'supermarket', orderBy: 'store_id' },
    item:              { db: 'supermarket', orderBy: 'sku' },
    inventory:         { db: 'supermarket', orderBy: 'store_id, sku' },
    store_transaction: { db: 'supermarket', orderBy: 'transaction_id DESC' },
    item_sale:         { db: 'supermarket', orderBy: 'transaction_id DESC, sku' },
    account:           { db: 'bank',        orderBy: 'account_number' },
};

const POOLS = { supermarket: supermarketPool, bank: bankPool };

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
app.listen(3000, () => console.log(`Supermarket API running on http://localhost:3000`));
