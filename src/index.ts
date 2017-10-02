import { Pool, Client, PoolConfig } from 'pg';
import { get } from 'lodash';

const { escapeIdentifier, escapeLiteral } = Client.prototype;

export interface Handler { (payload: any, ack: () => void, nack: (error: Error) => void): Promise<void> };

export interface QueueConfig {
  pool: PoolConfig;
  poll_interval: number;
  lock_timeout: number;
};

export interface PublishConfig {
  delay?: number;
}

export const make_client = async (config: QueueConfig) => {
  const pool = new Pool(config.pool);

  await pool.query(`CREATE TABLE IF NOT EXISTS paused (queue VARCHAR primary key, state BOOLEAN)`);

  const get_ready_job = async (queue_name: string) => {
    const name = escapeIdentifier(queue_name);
    const name_literal = escapeLiteral(queue_name);
    const lock_timeout = escapeLiteral(config.lock_timeout.toString());
    const results = await pool.query(`
      UPDATE ${name}
      SET status='locked', modified=CURRENT_TIMESTAMP
      FROM (SELECT paused FROM paused WHERE name=${name_literal}) paused
      WHERE id=(
        SELECT id FROM ${name} WHERE ready <= CURRENT_TIMESTAMP ORDER BY created LIMIT 1
      )
      AND paused.paused='FALSE'
      AND (
        status='ready'
        OR (
          EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - modified)) > ${lock_timeout}
          AND status='locked'
        )
      )
      RETURNING payload, id
    `);
    if (results.rowCount < 1) {
      return;
    }
    const id = get<number>(results, 'rows[0].id');
    const payload = JSON.parse(get<string>(results, 'rows[0].payload'));
    return { id, payload };
  };

  const make_ack = (queue_name: string, id: number) => async () => {
    try {
      const name = escapeIdentifier(queue_name);
      await pool.query(`DELETE FROM ${name} WHERE id=${id}`);
    } catch (error) {
      console.error(error);
    }
  };

  const make_nack = (queue_name: string, id: number) => async () => {
    try {
      const name = escapeIdentifier(queue_name);
      await pool.query(`UPDATE ${name} SET status='ready' WHERE id=${id}`);
    } catch (error) {
      console.error(error);
    }
  };

  const process_job = async (queue_name: string, handler: Handler) => {
    try {
      const job = await get_ready_job(queue_name);
      if (!job) return;
      await handler(job.payload, make_ack(queue_name, job.id), make_nack(queue_name, job.id));
    } catch (error) {
      console.error(error);
    }
  };

  const assert_queue = async (queue_name: string) => {
    const name = escapeIdentifier(queue_name);
    await pool.query(`
      CREATE TABLE IF NOT EXISTS ${name} (
        id serial primary key, status VARCHAR, payload VARCHAR,
        created TIMESTAMP, modified TIMESTAMP, ready TIMESTAMP
      );
    `);
    await pool.query(`ALTER TABLE ${name} ALTER COLUMN created SET DEFAULT CURRENT_TIMESTAMP`);
    await pool.query(`ALTER TABLE ${name} ALTER COLUMN modified SET DEFAULT CURRENT_TIMESTAMP`);
    await pool.query(`ALTER TABLE ${name} ALTER COLUMN status SET DEFAULT 'ready'`);
  };

  const sleep = (timeout: number) => new Promise(resolve => setTimeout(resolve, timeout));

  return {
    make_queue: async (queue_name: string, options = { paused: false }) => {
      const name = escapeLiteral(queue_name);
      const paused = options.paused ? 'TRUE' : 'FALSE';
      await assert_queue(queue_name);
      await pool.query(`
        INSERT INTO paused (name, paused)
        VALUES (${name}, ${paused})
        ON CONFLICT (name) DO UPDATE SET paused=${paused}
      `);
    },
    pause: async (queue_name: string) => {
      const name = escapeLiteral(queue_name);
      await pool.query(`
        INSERT INTO paused (name, paused)
        VALUES (${name}, 't')
        ON CONFLICT (name) DO UPDATE SET paused='t'
      `);
    },
    resume: async (queue_name: string) => {
      const name = escapeLiteral(queue_name);
      await pool.query(`
        INSERT INTO paused (name, paused)
        VALUES (${name}, 'f')
        ON CONFLICT (name) DO UPDATE SET paused='f'
      `);
    },
    publish: async (queue_name: string, options: PublishConfig = {}, data: any) => {
      const name = escapeIdentifier(queue_name);
      const payload = escapeLiteral(JSON.stringify(data));
      const ready_date = `CURRENT_TIMESTAMP + INTERVAL '${Number(options.delay) || 0} milliseconds'`;
      await assert_queue(queue_name);
      await pool.query(`
        INSERT INTO ${name} (payload, ready) values (${payload}, ${ready_date})
      `);
    },
    subscribe: async (queue_name: string, handler: Handler) => {
      await assert_queue(queue_name);
      (async () => {
        while (true) {
          await process_job(queue_name, handler);
          await sleep(config.poll_interval);
        }
      })();
      return;
    }
  }
}

