# Supermarket Chain Database Web App

COSC 3380 database web app, team **TODO: team number**. It simulates a
supermarket chain (100+ stores across several states) with a separate bank
database for card payments.

- **Demo video:** TODO: public link
- **Report:** `teamXY.pdf` (TODO: rename to match the actual file)

## Team

| Member | Responsibilities |
|---|---|
| Matthew Dulworth | TODO |
| Javier Puentes Leiva | TODO |

## Requirements

- PostgreSQL (tested with 18), running locally
- Node.js 20.12 or newer

## Setup

1. **Create two databases.** The supermarket and the bank are separate
   databases. Any names work; for example:

   ```sh
   createdb supermarket
   createdb bank
   ```

2. **Create `src/.env`.** The server reads its connection settings from this
   file and will not start without it:

   ```sh
   DB_USER=postgres
   DB_PASSWORD=your-password
   DB_HOST=localhost
   DB_PORT=5432
   DB_SUPERMARKET=supermarket
   DB_BANK=bank
   ```

   `DB_SUPERMARKET` and `DB_BANK` must match the database names from step 1.

3. **Install dependencies and start the server:**

   ```sh
   cd src
   npm install
   npm start
   ```

   It prints `Supermarket API running on http://localhost:3000`. Leave it
   running.

4. **Open `src/home.html`** in a browser (double-click it; no web server is
   needed).

5. **Load the data.** On first run the tables do not exist yet:
   - **Supermarket Backend** tab, then **Reset Supermarket Tables**: creates the
     tables from `ddl/ddl.sql` and generates the stores, items, inventory and
     past sales.
   - **Bank** tab, then **Reset Bank Tables**: creates the bank accounts from
     `ddl/bankddl.sql`.

   Both buttons ask for confirmation, and both can be used again later to
   start over.

## Using the app

- **Customer Checkout:** the cash register. Pick a store, add items to the cart
  and pay by card (bank account number) or cash. **Simulate Random Checkout**
  picks a random store, 10-20 random items and a random payment method, so no
  typing is needed.
- **Supermarket Backend:** the back office. Monthly sales per store, top-selling
  item per department, and a low-stock reorder list, optionally filtered by
  store.
- **Bank:** account balances, which drop after each card payment.
- **SQL Sent to Server** (right side): every SQL statement each request ran,
  with its database and time in milliseconds, newest first. Checkouts show
  the full transaction, including `ROLLBACK` when one fails.

Every checkout runs as a single transaction across both databases: it locks
the inventory rows, records the sale and its line items, decrements stock and
charges the bank account, and rolls everything back if any step fails (for
example, insufficient funds or not enough stock).

## Files

| Path | Contents |
|---|---|
| `src/home.html` | The web app (open this) |
| `src/server.js` | Node/Express API; all SQL is built and sent here |
| `ddl/ddl.sql` | Supermarket tables and generated data |
| `ddl/bankddl.sql` | Bank tables and generated accounts |
| `transaction.sql` | Trace of the checkout transaction SQL, written by the server |
| `query.sql` | Trace of the report query SQL, written by the server |
| `receipts/` | The five supermarket receipts the design is based on |
| `writeup/` | ER diagrams (draft, preliminary, final) and screenshots |
