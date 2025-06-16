# NixIndexer

A minimal solana indexer that fetches and stores Solana program transactions and account data in a PostgreSQL database.

## Features

- Fetches transactions for specific Solana programs
- Parses and stores instruction data, fee payers, and account information
- Indexes program accounts and their associated data
- Stores all data in a structured PostgreSQL database


## Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd solana-postgres-indexer
```

2. Install dependencies:
```bash
npm install
```

3. Create a `.env` file in the root directory with the following variables:
```env
POSTGRES_URL=your_postgres_connection_string
RPC_URL=your_solana_rpc_endpoint
```

## Running the Project

1. Build the project:
```bash
npm run build
```

2. Start the indexer:
```bash
npm start
```

For development with hot-reload:
```bash
npm run dev
```

## Database Schema

The indexer creates tables dynamically for each program being tracked with the following structure:
- `txn_[programId]` table:
  - `id`: Serial primary key
  - `txn_sig`: Transaction signature
  - `ixn_data`: JSONB instruction data
  - `fee_payer`: Fee payer address
  - `name`: Instruction name
  - `accounts`: Array of account addresses involved
