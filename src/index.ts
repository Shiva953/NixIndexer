import postgres from 'postgres';
import pg, { Client } from "pg";

// GET PROGRAM ID -> GET ALL TXNS FOR THAT PROGRAM ID(USE RPC METHODS TO QUERY) + PARSE EACH TXN 
// FOR GIVEN (PROGRAM ID, TXN) -> FILTER ALL IXNS IN THAT TXN, (TAKE THE IXN DATA+NAME+FEE_PAYER+..) FOR EACH IXN FROM ITS MESSAGE HEADER
// IXN <-> (DATA, NAME, FEE_PAYER, ...) -> PUSH TO POSTGRES DB IN txs_program_{programId} TABLE

type DBElement = {
    ixnData: object;
    feePayer: string | null;
    name: string | null;
    accounts: string[];
};

function getDb(dbUrl: string) {
	return postgres(dbUrl);
}

// //get + parse txns for a given program id
// async function getTxnsForProgramId(programId: string) {
    
// }

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
            method: "getParsedTransaction",
            params: [txnSig, "confirmed"]
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

    const message = parsedTxn.transaction.message;
    const accountKeys: string[] = message.accountKeys;
    const instructions = message.instructions;
    const signatures = Array.isArray(parsedTxn.transaction.signatures) ? parsedTxn.transaction.signatures : [];
    const feePayer = accountKeys[0] || null; 
    const meta = parsedTxn.meta && typeof parsedTxn.meta === "object" ? parsedTxn.meta : {};
    const fee = meta.fee || null;


    const ixnArray: DBElement[] = instructions.map((ixn: any): DBElement => {
        const programIdIndex = ixn.programIdIndex;
        const programIdStr = typeof programIdIndex === "number" && accountKeys[programIdIndex] ? accountKeys[programIdIndex] : null;
        const accounts = Array.isArray(ixn.accounts) ? ixn.accounts.map((idx: number) => accountKeys[idx]) : [];
        return {
            ixnData: ixn, // raw instruction object
            feePayer: feePayer,
            name: programIdStr,
            accounts: accounts
        };
    });

    return ixnArray;
}

async function ParseAccountsAndUpsertToDB(rpcUrl: string, programId:string){
    // get associated accounts owned by the programId
    // parse each account
    // return the account[] array
    // make a new table program_{programId}
    // each row would contain new account data

}

// take that (ixn data,name,fee_payer,accounts) and push them to db
// create different columns for each ixn data(JSONB) feePayer(text), name(text), accounts(text[]), use the same table txn-{programId} and add each new one row wise 
async function upsertTransactionWithToDBWithInstructions(txnSig: string, programId: string, rpcUrl: string, pgUrl: string) {
    const rows = await parseInstructionsForGivenTransaction(rpcUrl, txnSig);
    const tableName = `txn_${programId.replace(/[^a-zA-Z0-9_]/g, "_")}`;

    const client = new Client({ connectionString: pgUrl });
    await client.connect();

    // Create table if not exists
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

    // Insert each row
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

    await client.end();
    console.log("Succesfully inserted instructions into the table!")
}

async function main() {
    try {
        const requiredEnvVars = {
            POSTGRES_URL: process.env.POSTGRES_URL,
            RPC_URL: process.env.RPC_URL
        };

        const txnSig = '2nBhEBYYvfaAe16UMNqRHre4YNSskvuYgx3M6E4JP1oDYvZEJHvoPzyUidNgNX5r9sTyN1J9UxtbCXy2rqYcuyuv';
        const programId = ''

        const missingEnvVars = Object.entries(requiredEnvVars)
            .filter(([_, value]) => !value)
            .map(([key]) => key);

        if (missingEnvVars.length > 0) {
            throw new Error(`Missing required environment variables: ${missingEnvVars.join(', ')}`);
        }

        const { POSTGRES_URL, RPC_URL } = requiredEnvVars as { POSTGRES_URL: string; RPC_URL: string };

        console.log('Environment variables validated successfully');

        await upsertTransactionWithToDBWithInstructions(txnSig, programId, RPC_URL, POSTGRES_URL);
        console.log("Successfully indexed instructions from a solana txn")

        
    } catch (error) {
        console.error('Error:', error);
        process.exit(1);
    }
}

main(); 