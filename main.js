import fetch from 'node-fetch';
import pg from 'pg';
import dotenv from 'dotenv';
dotenv.config();


const { Client } = pg;
// get the password from the environment variable
const postgresPassword = process.env.POSTGRES_PASSWORD;
const arweaveEndpoint = 'https://arweave.net/graphql';
const postgresUri = `postgres://postgres:${postgresPassword}@localhost:5432/darkblock`;

const walletAddress = 'fmTpIBFrCbAyUjV-f3VOq7Szs5RaMovb1Pf9MlUnjVk';
const batchSize = 100;
const delay = 200;

async function main() {
  const client = new Client({
    connectionString: postgresUri
  });

  try {
    console.log('Connecting to the database...');
    await client.connect();
    console.log('Connected to the database.');

    console.log('Creating transactions table if it does not exist...');
    await client.query(`
    CREATE TABLE IF NOT EXISTS transactions (
    id TEXT PRIMARY KEY,
    data JSONB,
    cursor TEXT,
    transaction_order INTEGER
    )
    `);
    console.log('Transactions table created or already exists.');

    let hasMoreTransactions = true;
    let endCursor = '';
    let transactionOrder = 0;
    const { rows } = await client.query(`
    SELECT *
    FROM transactions
    ORDER BY transaction_order DESC
    LIMIT 1;
    `);

      if (rows.length > 0 && rows[0].cursor !== undefined) {
        endCursor = rows[0].cursor;
        transactionOrder = rows[0].transaction_order;
      }

    // wait for one second before starting the loop
    await new Promise(resolve => setTimeout(resolve, 1000));

    while (hasMoreTransactions) {
      
      
      console.log("transaction order in database: " + transactionOrder);
      console.log("lateset cursor in databse: " + endCursor);
      const query = `
        query {
          transactions(
            owners: ["${walletAddress}"],
            first: ${batchSize},
            after: "${endCursor}"
          ) {
            pageInfo {
              hasNextPage
            }
            edges {
              cursor
              node {
                id
                tags {
                  name
                  value
                }
              }
            }
          }
        }
      `;

      console.log(`Fetching transactions with cursor ${endCursor}...`);
      const response = await fetch(arweaveEndpoint, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ query })
      });
      if (response.ok) {
      const { data, errors } = await response.json();

      if (errors) {
        console.error(errors);
        throw errors;
        // return;
      }

      const { transactions } = data;
      // console.log(transactions.edges.map(edge => edge.node.id));

      const { edges, pageInfo } = transactions;
      
      if (edges.length > 0) {
        const values = [];

        for (const { cursor, node } of edges) {
          console.log(node)
          transactionOrder++;
          const { id, tags } = node;
          const data = {
            id,
            tags: tags.reduce((acc, { name, value }) => {
              acc[name] = value;
              return acc;
            }, {}),
            cursor,
            transaction_order: transactionOrder,
          };
          values.push(data);


          // console.log(`Adding row with values ${JSON.stringify(data)}...`);
        }

        try {
          if (values.length > 0) {
            const placeholders = Array.from({ length: values.length }, (_, i) =>
              `($${i * 4 + 1}, $${i * 4 + 2}, $${i * 4 + 3}, $${i * 4 + 4})`
            ).join(', ');

            const insertParams = values.flatMap(({ id, tags, cursor, transaction_order }) => [id, tags, cursor, transaction_order]);

            await client.query(`
              INSERT INTO transactions (id, data, cursor, transaction_order)
              VALUES ${placeholders}
              ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data, cursor = EXCLUDED.cursor, transaction_order = EXCLUDED.transaction_order;
            `, insertParams);
          }

          endCursor = edges[edges.length - 1].cursor;
          console.log(`Inserted ${values.length} rows in the database. Fetching next ${batchSize} transactions with cursor ${endCursor}...`);
          hasMoreTransactions = pageInfo.hasNextPage;
          if (!pageInfo.hasNextPage) {
            console.log('No more transactions found. Waiting for 10 minutes before checking again.');
            hasMoreTransactions = false;
            return;
          }
          await new Promise(resolve => setTimeout(resolve, delay));
        } catch (error) {
          console.error(error);
          throw error;
          // return;
        }
      } else {
        console.log('No more transactions found. Waiting for 10 minutes before checking again.');
        // wait 5 seconds before checking again
        await new Promise(resolve => setTimeout(resolve, delay * 10));
      }
    }
  } } catch (error) {
    console.error(error);
  } finally {
    await client.end();
  }
}

// main().catch(console.log);
// console.log('Done.');

async function mainWithRetries() {
  const maxRetries = 3; // maximum number of retries
  const retryDelay = 5000; // initial delay in milliseconds before retrying
  let attempt = 1;

  while (attempt <= maxRetries) {
    try {
      await main();
      console.log('Done.');
      return;
    } catch (error) {
      console.error(`Attempt ${attempt} failed with error: ${error.message}`);
      if (shouldRetry(error) && attempt < maxRetries) {
        const delay = retryDelay * Math.pow(2, attempt - 1);
        console.log(`Retrying in ${delay / 1000} seconds...`);
        await new Promise(resolve => setTimeout(resolve, delay));
      } else {
        console.error(`All attempts failed after ${attempt} retries.`);
        throw error;
      }
      attempt++;
    }
  }
}

function shouldRetry(error) {
  // Only retry on specific types of errors
  return error.message.includes('Network Error');
}

mainWithRetries().catch(console.log);



