import { Pool, Client, PoolConfig } from 'pg';
import { get } from 'lodash';

const { escapeIdentifier, escapeLiteral } = Client.prototype;

interface IHandler { (payload: any, ack: () => void, nack: (error: Error) => void): void };

interface IConfig {
  pool: PoolConfig;
  poll_interval: number;
  lock_timeout: number;
};

interface IPublishOptions {
  delay?: number;
}

export const make_client = async (config: IConfig) => {
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

  const process_job = async (queue_name: string, handler: IHandler) => {
    try {
      const job = await get_ready_job(queue_name);
      if (!job) return;
      handler(job.payload, make_ack(queue_name, job.id), make_nack(queue_name, job.id));
    } catch (error) {
      console.error(error);
    }
  };

  const sleep = (timeout: number) => new Promise(resolve => setTimeout(resolve, timeout));

  return {
    make_queue: async (queue_name: string, options = { paused: false }) => {
      const name = escapeIdentifier(queue_name);
      const name_literal = escapeLiteral(queue_name);
      await pool.query(`
        CREATE TABLE IF NOT EXISTS ${name} (
          id serial primary key, status VARCHAR, payload VARCHAR,
          created TIMESTAMP, modified TIMESTAMP, ready TIMESTAMP
        );
      `);
      await pool.query(`
        INSERT INTO paused (name, paused)
        VALUES (${name_literal}, ${options.paused ? 'TRUE' : 'FALSE'})
        ON CONFLICT (name) DO NOTHING
      `);
      await pool.query(`ALTER TABLE ${name} ALTER COLUMN created SET DEFAULT CURRENT_TIMESTAMP`);
      await pool.query(`ALTER TABLE ${name} ALTER COLUMN modified SET DEFAULT CURRENT_TIMESTAMP`);
      await pool.query(`ALTER TABLE ${name} ALTER COLUMN status SET DEFAULT 'ready'`);
    },
    pause_queue: async (queue_name: string) => {
      const name = escapeLiteral(queue_name);
      await pool.query(`UPDATE paused SET paused='t' WHERE name=${name}`);
    },
    resume_queue: async (queue_name: string) => {
      const name = escapeLiteral(queue_name);
      await pool.query(`UPDATE paused SET paused='f' WHERE name=${name}`);
    },
    publish: async (queue_name: string, options: IPublishOptions = {}, data: any) => {
      const name = escapeIdentifier(queue_name);
      const payload = escapeLiteral(JSON.stringify(data));
      const ready_date = `CURRENT_TIMESTAMP + INTERVAL '${Number(options.delay) || 0} milliseconds'`;
      await pool.query(`
        INSERT INTO ${name} (payload, ready) values (${payload}, ${ready_date})
      `);
    },
    subscribe: async (queue_name: string, handler: IHandler) => {
      while (true) {
        await process_job(queue_name, handler);
        await sleep(config.poll_interval);
      }
    }
  }
}

