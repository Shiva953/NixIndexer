// GET PROGRAM ID -> GET ALL TXNS FOR THAT PROGRAM ID(USE RPC METHODS TO QUERY) + PARSE EACH TXN 
// FOR GIVEN (PROGRAM ID, TXN) -> FILTER ALL IXNS IN THAT TXN, (TAKE THE IXN DATA+NAME+FEE_PAYER+..) FOR EACH IXN FROM ITS MESSAGE HEADER
// IXN <-> (DATA, NAME, FEE_PAYER, ...) -> PUSH TO POSTGRES DB IN txs_program_{programId} TABLE

async function main() {
    try {

        //get + parse txns for a given program id
        async function getTxnsForProgramId(programId: string) {
            
        }

        // FOR GIVEN PROGRAM ID + TXN, FILTER and parse and get the (ixn data,name,fee_payer, accounts involved in that)
        async function upsertToDB(txns: any[]) {


        }

        // take that (ixn data,name,fee_payer,accounts) and push them to db
        // create different columns for each ixn data(JSONB) feePayer(text), name(text), accounts(text[]), use the same table txn-{programId} and add each new one row wise 

        
    } catch (error) {
        console.error('Error:', error);
        process.exit(1);
    }
}

main(); 