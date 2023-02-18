import fetch from 'node-fetch';
import pg from 'pg';
import dotenv from 'dotenv';
dotenv.config();


const { Client } = pg;
// get the password from the environment variable
const postgresPassword = process.env.POSTGRES_PASSWORD;
console.log("postgres password: "+postgresPassword);
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
    cursor TEXT
    )
    `);
    console.log('Transactions table created or already exists.');

    let hasMoreTransactions = true;
    let endCursor = '';
    let cursor = '';  
    const { rows } = await client.query(`
      SELECT cursor
      FROM transactions
      ORDER BY data->>'timestamp' DESC
      LIMIT 1;
    `);

    if (rows.length > 0 && rows[0].cursor !== undefined) {
      cursor = rows[0].cursor;
    }
    console.log("lateset cursor in databse: "+cursor);
    while (hasMoreTransactions) {
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

      const { data, errors } = await response.json();

      if (errors) {
        console.error(errors);
        return;
      }

      const { transactions } = data;
      // console.log(transactions.edges.map(edge => edge.node.id));

      const { edges, pageInfo } = transactions;

      if (edges.length > 0) {
        const values = [];

        for (const { cursor, node } of edges) {
          const { id, tags } = node;
          const data = {
            id,
            tags: tags.reduce((acc, { name, value }) => {
              acc[name] = value;
              return acc;
            }, {}),
            cursor,
          };
          values.push(data);
          // console.log(`Adding row with values ${JSON.stringify(data)}...`);
        }

        try {
          if (values.length > 0) {
            const placeholders = Array.from({ length: values.length }, (_, i) =>
              `($${i * 3 + 1}, $${i * 3 + 2}, $${i * 3 + 3})`
            ).join(', ');

            const insertParams = values.flatMap(({ id, tags, cursor }) => [id, tags, cursor]);

            await client.query(`
            INSERT INTO transactions (id, data, cursor)
            VALUES ${placeholders}
            ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data, cursor = EXCLUDED.cursor;
          `, insertParams);
          }

          endCursor = edges[edges.length - 1].cursor;
          console.log(`Inserted ${values.length} rows in the database. Fetching next ${batchSize} transactions with cursor ${endCursor}...`);
          hasMoreTransactions = pageInfo.hasNextPage;
          if (!pageInfo.hasNextPage) {
            console.log('No more transactions found. Waiting for 10 minutes before checking again.');
            hasMoreTransactions = false;
            await new Promise(resolve => setTimeout(resolve, 600000));
          }
          await new Promise(resolve => setTimeout(resolve, delay));
        } catch (error) {
          console.error(error);
          return;
        }
      }
    }
  } catch (error) {
    console.error(error);
  } finally {
    await client.end();
  }
}

main().catch(console.log);
console.log('Done.');


