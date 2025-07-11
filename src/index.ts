import postgres from 'postgres';
import pg, { Client } from "pg";
import dotenv from 'dotenv';
import path from 'path';
import { Connection, PublicKey } from "@solana/web3.js";

dotenv.config({ path: path.resolve(__dirname, '../.env') });

type ProgramAccount = {
    pubkey: string;
    account: {
        lamports: number;
        owner: string;
        data: any;
        executable: boolean;
        rentEpoch: number;
        space: number;
    };
};

type ParsedAccount = {
    accountAddress: string;
    owner: string;
    data: any;
    solBalance: number;
};

type RPCResponse = {
    jsonrpc: string;
    id: number;
    result: ProgramAccount[];
};

type DBElement = {
    ixnData: object;
    feePayer: string | null;
    name: string | null;
    accounts: string[];
};

// GET PROGRAM ID -> GET ALL TXNS FOR THAT PROGRAM ID(USE RPC METHODS TO QUERY) + PARSE EACH TXN 
// FOR GIVEN (PROGRAM ID, TXN) -> FILTER ALL IXNS IN THAT TXN, (TAKE THE IXN DATA+NAME+FEE_PAYER+..) FOR EACH IXN FROM ITS MESSAGE HEADER
// IXN <-> (DATA, NAME, FEE_PAYER, ...) -> PUSH TO POSTGRES DB IN txs_program_{programId} TABLE

async function rpcFetch(rpcUrl: string, txnSig: string) {
    try {
        if (!rpcUrl || typeof rpcUrl !== "string") {
            throw new Error("Invalid rpcUrl provided");
        }
        if (!txnSig || typeof txnSig !== "string") {
            throw new Error("Invalid txnSig provided");
        }

        const body = {
            jsonrpc: "2.0",
            id: 1,
            method: "getTransaction",
            params: [txnSig, {"encoding": "json", "maxSupportedTransactionVersion":0, commitment: "finalized"}]
        };

        const response = await fetch(rpcUrl, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(body)
        });

        if (!response.ok) {
            throw new Error(`Network response was not ok: ${response.status} ${response.statusText}`);
        }

        let data: any;
        data = await response.json();
        if (!data || typeof data !== "object" || ("error" in data && data.error) || !("result" in data) || data.result == null) {
            throw new Error("Invalid RPC response: " + JSON.stringify(data));
        }

        return data.result;
    } catch (err) {
        console.error(`rpcFetch error for txnSig ${txnSig}:`, err);
        throw err;
    }
}

// FOR GIVEN PROGRAM ID + TXN, FILTER and parse and get the (ixn data,name,fee_payer, accounts involved in that)
async function parseInstructionsForGivenTransaction(rpcUrl: string, txnSig: string) {
    try {
        const parsedTxn = await rpcFetch(rpcUrl, txnSig);

        if (!parsedTxn || typeof parsedTxn !== "object") {
            throw new Error("rpcFetch did not return a valid transaction object");
        }
        if (!parsedTxn.transaction || typeof parsedTxn.transaction !== "object") {
            throw new Error("Transaction object missing in parsed transaction");
        }
        if (!parsedTxn.transaction.message || typeof parsedTxn.transaction.message !== "object") {
            throw new Error("Transaction message missing in parsed transaction");
        }
        if (!Array.isArray(parsedTxn.transaction.message.instructions)) {
            throw new Error("Instructions array missing in transaction message");
        }
        if (!Array.isArray(parsedTxn.transaction.message.accountKeys)) {
            throw new Error("Account keys array missing in transaction message");
        }

        const message = parsedTxn?.transaction?.message;
        const accountKeys: string[] = message.accountKeys;
        const instructions = message?.instructions || [];
        const signatures = Array.isArray(parsedTxn.transaction.signatures) ? parsedTxn.transaction.signatures : [];
        const feePayer = accountKeys[0] || null; 
        const meta = parsedTxn.meta && typeof parsedTxn.meta === "object" ? parsedTxn.meta : {};
        const fee = meta.fee || null;

        let ixnArray: DBElement[] = [];
        try {
            const instructionPromises = instructions.map((ixn: any): Promise<DBElement> => {
                return Promise.resolve({
                    ixnData: ixn, // raw instruction object
                    feePayer: feePayer,
                    name: typeof ixn.programIdIndex === "number" && accountKeys[ixn.programIdIndex] ? accountKeys[ixn.programIdIndex] : null,
                    accounts: Array.isArray(ixn.accounts) ? ixn.accounts.map((idx: number) => accountKeys[idx]) : []
                });
            });
            
            ixnArray = await Promise.all(instructionPromises);
        } catch (err) {
            console.error("Error while mapping instructions:", err);
            throw err;
        }

        return ixnArray;
    } catch (err) {
        console.error("Error in parseInstructionsForGivenTransaction:", err);
        throw err;
    }
}


async function getProgramAccounts(rpcUrl: string, programId: string): Promise<ProgramAccount[]> {
    try {
        if (!rpcUrl || typeof rpcUrl !== "string") {
            throw new Error("Invalid rpcUrl provided");
        }
        if (!programId || typeof programId !== "string") {
            throw new Error("Invalid programId provided");
        }

        const body = {
            jsonrpc: "2.0",
            id: 1,
            method: "getProgramAccounts",
            params: [
                programId,
                {
                    encoding: "base64",
                    limit: 10
                }
            ]
        };

        const response = await fetch(rpcUrl, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(body)
        });

        if (!response.ok) {
            throw new Error(`Network response was not ok: ${response.status} ${response.statusText}`);
        }

        const data = await response.json() as RPCResponse;
        if (!data || typeof data !== "object" || ("error" in data && data.error) || !("result" in data)) {
            throw new Error("Invalid RPC response: " + JSON.stringify(data));
        }

        const programAccounts = data.result
        if(programAccounts.length > 10){
            return programAccounts.slice(0,10)
        }
        else{
            return programAccounts
        }

        // return data.result;
    } catch (err) {
        console.error(`getProgramAccounts error for programId ${programId}:`, err);
        throw err;
    }
}

async function ParseAccountsAndUpsertToDB(rpcUrl: string, programId: string): Promise<ParsedAccount[]> {
    try {
        const LAMPORTS_PER_SOL = 1000000000;
        const accounts = await getProgramAccounts(rpcUrl, programId);
        
        const parsedAccounts = accounts.map((account: ProgramAccount) => ({
            accountAddress: account.pubkey,
            owner: account.account.owner,
            data: account.account.data,
            solBalance: account.account.lamports / LAMPORTS_PER_SOL
        }));

        return parsedAccounts;
    } catch (error) {
        console.error('Error in ParseAccountsAndUpsertToDB:', error);
        throw error;
    }
}

// take that (ixn data,name,fee_payer,accounts) and push them to db
// create different columns for each ixn data(JSONB) feePayer(text), name(text), accounts(text[]), use the same table txn-{programId} and add each new one row wise 
async function upsertTransactionWithToDBWithInstructions(txnSig: string, programId: string, rpcUrl: string, pgUrl: string) {
    let client: Client | null = null;
    try {
        const rows = await parseInstructionsForGivenTransaction(rpcUrl, txnSig);
        
        if (!rows || rows.length === 0) {
            console.log(`No instructions found for transaction ${txnSig}`);
            return;
        }

        const tableName = `txn_${programId.replace(/[^a-zA-Z0-9_]/g, "_")}`;

        client = new Client({ connectionString: pgUrl });
        await client.connect();

        const createTableQuery = `
            CREATE TABLE IF NOT EXISTS "${tableName}" (
                id SERIAL PRIMARY KEY,
                txn_sig TEXT NOT NULL,
                ixn_data JSONB NOT NULL,
                fee_payer TEXT,
                name TEXT,
                accounts TEXT[]
            );
        `;
        await client.query(createTableQuery);

        for (const row of rows) {
            const insertQuery = `
                INSERT INTO "${tableName}" (txn_sig, ixn_data, fee_payer, name, accounts)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT DO NOTHING;
            `;
            await client.query(insertQuery, [
                txnSig,
                JSON.stringify(row.ixnData),
                row.feePayer,
                row.name,
                row.accounts
            ]);
        }

        console.log(`Successfully inserted ${rows.length} instructions into table ${tableName}`);
    } catch (error) {
        console.error('Error in upsertTransactionWithToDBWithInstructions:', error);
        throw error;
    } finally {
        if (client) {
            try {
                await client.end();
            } catch (err) {
                console.error('Error closing database connection:', err);
            }
        }
    }
}

async function upsertProgramAssociatedAccountsToDB(programId: string, rpcUrl: string, pgUrl: string) {
    const connection = new Connection(rpcUrl, "confirmed");

    const tableName = `accounts_new_maxlimited_${programId.replace(/[^a-zA-Z0-9_]/g, "_")}`;
    let client: Client | null = null;

    try {
        client = new Client({ connectionString: pgUrl });
        await client.connect();

        const createTableQuery = `
            CREATE TABLE IF NOT EXISTS "${tableName}" (
                id SERIAL PRIMARY KEY,
                pubkey TEXT NOT NULL UNIQUE,
                owner TEXT NOT NULL,
                lamports BIGINT NOT NULL,
                executable BOOLEAN NOT NULL,
                rent_epoch NUMERIC NOT NULL,
                data BYTEA NOT NULL,
                space BIGINT
            );
        `;
        await client.query(createTableQuery);

        const accounts = await getProgramAccounts(rpcUrl, programId);

        for (const acct of accounts) {
            const { pubkey, account } = acct;
            const LAMPORTS_PER_SOL = 1000000000;
            const rentEpochInSol = (typeof account.rentEpoch === 'number' ? account.rentEpoch : 0) / LAMPORTS_PER_SOL;
            const dataBuffer = Array.isArray(account.data) && account.data.length > 0 && account.data[1] === 'base64'
                ? Buffer.from(account.data[0], 'base64')
                : Buffer.from([]);
            const insertQuery = `
                INSERT INTO "${tableName}" (pubkey, owner, lamports, executable, rent_epoch, data, space)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (pubkey) DO UPDATE SET
                    owner = EXCLUDED.owner,
                    lamports = EXCLUDED.lamports,
                    executable = EXCLUDED.executable,
                    rent_epoch = EXCLUDED.rent_epoch,
                    data = EXCLUDED.data,
                    space = EXCLUDED.space;
            `;
            await client.query(insertQuery, [
                pubkey,
                account.owner,
                account.lamports,
                account.executable,
                rentEpochInSol,
                dataBuffer,
                account.space
            ]);
        }

        console.log(`Successfully upserted ${accounts.length} accounts into table ${tableName}`);
    } catch (error) {
        console.error('Error in upsertProgramAssociatedAccountsToDB:', error);
        throw error;
    } finally {
        if (client) {
            try {
                await client.end();
            } catch (err) {
                console.error('Error closing database connection:', err);
            }
        }
    }
}
async function main() {
    try {
        const requiredEnvVars = {
            POSTGRES_URL: process.env.POSTGRES_URL,
            RPC_URL: process.env.RPC_URL
        };

        // Accept arguments from command line
        const args = process.argv.slice(2);
        if (args.length < 2) {
            console.error('Usage: ts-node src/index.ts <PROGRAM_ID> <TXN_SIG>');
            process.exit(1);
        }
        const [programId, txnSig] = args;

        const missingEnvVars = Object.entries(requiredEnvVars)
            .filter(([_, value]) => !value)
            .map(([key]) => key);

        if (missingEnvVars.length > 0) {
            throw new Error(`Missing required environment variables: ${missingEnvVars.join(', ')}`);
        }

        const { POSTGRES_URL, RPC_URL } = requiredEnvVars as { POSTGRES_URL: string; RPC_URL: string };

        console.log('Environment variables validated successfully');
        console.log(`Using Program ID: ${programId}`);
        console.log(`Using Transaction Signature: ${txnSig}`);

        await upsertTransactionWithToDBWithInstructions(txnSig, programId, RPC_URL, POSTGRES_URL);
        await upsertProgramAssociatedAccountsToDB(programId, RPC_URL, POSTGRES_URL)
        console.log("Successfully indexed instructions from a solana txn")

        
    } catch (error) {
        console.error('Error:', error);
        process.exit(1);
    }
}
main(); 

