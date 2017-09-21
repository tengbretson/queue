import { Pool, Notification, PoolConfig } from 'pg';
import { get } from 'lodash';

const pool = new Pool({
  user: 'root',
  password: 'password',
  database: 'queue',
  host: '192.168.1.1',
  port: 5433,
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
});

interface IHandler {
  (payload: any, ack: () => void, nack: (error: Error) => void): void
}

export const make_client = (config: PoolConfig) => {
  const pool = new Pool(config);

  return {
    make_queue: async (queue_name: string) => {
      const client = await pool.connect();
      await client.query(`
        CREATE TABLE IF NOT EXISTS ${client.escapeIdentifier(queue_name)} (
          id serial primary key,
          status varchar,
          payload varchar,
          created timestamp
        );
      `);
    
      await client.query(`
        ALTER TABLE ${client.escapeIdentifier(queue_name)}
        ALTER COLUMN created SET DEFAULT CURRENT_TIMESTAMP
      `);
      
      await client.query(`
        ALTER TABLE ${client.escapeIdentifier(queue_name)}
        ALTER COLUMN status SET DEFAULT 'ready'
      `);
    
      await client.query(`
        CREATE TABLE IF NOT EXISTS ${client.escapeIdentifier(queue_name + '_lock')} (
          id int primary key,
          locked boolean,
          created timestamp
        );
      `);
    
      await client.query(`
        ALTER TABLE ${client.escapeIdentifier(queue_name + '_lock')}
        ALTER COLUMN locked SET DEFAULT 'TRUE'
      `);
    
      await client.query(`
        ALTER TABLE ${client.escapeIdentifier(queue_name + '_lock')}
        ALTER COLUMN created SET DEFAULT CURRENT_TIMESTAMP
      `);
    
      await client.query(`
        CREATE OR REPLACE FUNCTION ${client.escapeIdentifier('notify_' + queue_name)}()
        RETURNS trigger AS $$
        DECLARE
        BEGIN
          PERFORM pg_notify(${client.escapeLiteral(queue_name)}, 'update');
          RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
      `);
    
      await client.query(`
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1
            FROM   pg_trigger
            WHERE  NOT tgisinternal AND tgname = ${client.escapeLiteral(queue_name + '_update')}
          ) THEN
          ELSE
            CREATE TRIGGER ${client.escapeIdentifier(queue_name + '_update')}
              AFTER UPDATE OR INSERT ON ${client.escapeIdentifier(queue_name)}
                FOR EACH ROW EXECUTE PROCEDURE ${client.escapeIdentifier('notify_' + queue_name)}();
           END IF;
        END
        $$
      `);
      client.release();
    },
    publish: async (queue_name: string, payload: any) => {
      const client = await pool.connect();
      await client.query(`
        INSERT INTO ${client.escapeIdentifier(queue_name)} (payload) values (
          ${client.escapeLiteral(JSON.stringify(payload))}
        )
      `);
      client.release();
    },
    subscribe: async (queue_name: string, handler: IHandler) => {
      const client = await pool.connect();
      client.on('notification', async ({ channel }) => {
        if (channel === queue_name) {
          await client.query('BEGIN');
          await client.query(`
            DELETE FROM ${client.escapeIdentifier(queue_name + '_lock')}
              WHERE EXTRACT(EPOCH FROM CURRENT_TIMESTAMP - created) > 30
          `)
          const result = await client.query(`
            SELECT ${client.escapeIdentifier(queue_name)}.id AS id, payload FROM
              ${client.escapeIdentifier(queue_name)}
            LEFT JOIN
              ${client.escapeIdentifier(queue_name + '_lock')}
            ON (${client.escapeIdentifier(queue_name)}.id = ${client.escapeIdentifier(queue_name + '_lock')}.id)
            WHERE status = 'ready'
            ORDER BY ${client.escapeIdentifier(queue_name)}.created
            LIMIT 1
          `);
          if (result.rowCount === 1) {
            const id = get<number>(result, 'rows[0].id');
            try {
              await client.query(`
                INSERT INTO ${client.escapeIdentifier(queue_name + '_lock')} (id) values (
                  ${client.escapeLiteral(id.toString())}
                )
              `);
              const payload = get<string>(result, 'rows[0].payload');
              const obj = JSON.parse(payload);
              const ack = async () => {
                await client.query('BEGIN');
                await client.query(`
                  UPDATE ${client.escapeIdentifier(queue_name)}
                  SET status='delivered'
                  WHERE id = ${client.escapeLiteral(id.toString())}
                `);
                await client.query(`
                  DELETE FROM ${client.escapeIdentifier(queue_name + '_lock')}
                  WHERE id = ${client.escapeLiteral(id.toString())}
                `);
                await client.query('COMMIT');
              }
      
              const nack = async () => {
                await client.query(`
                  DELETE FROM ${client.escapeIdentifier(queue_name + '_lock')}
                  WHERE id = ${client.escapeLiteral(id.toString())}
                `);
              }
      
              handler(obj, ack, nack);
            } catch (e) { } finally {
              await client.query('COMMIT');  
            }
          } else {
            return await client.query('COMMIT');
          }
        }
      })
      await client.query(`LISTEN ${client.escapeIdentifier(queue_name)}`);
      await client.query(`SELECT pg_notify(${client.escapeLiteral(queue_name)}, 'update')`);
    }
  }
}

