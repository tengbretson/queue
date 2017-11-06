import { get } from 'lodash';
import * as Knex from 'knex';
import { EventEmitter } from 'events';

export interface Handler { (payload: any): Promise<void> };

export interface QueueConfig {
  pool: Knex.PoolConfig;
  poll_interval: number;
  lock_timeout: number;
  max_parallel: number;
  connection: Knex.ConnectionConfig;
};

export interface PublishConfig {
  delay?: number;
}

const sleep = (t: number) => new Promise(resolve => setTimeout(resolve, t));

export const make_client = async (config: QueueConfig) => {
  const knex = Knex({
    client: 'pg',
    pool: config.pool,
    connection: config.connection
  });
  
  const CURRENT_TIMESTAMP = knex.raw('CURRENT_TIMESTAMP');

  knex.schema.createTableIfNotExists('paused', table => {
    table.string('queue').primary();
    table.boolean('state');
  });

  const get_job_to_complete = async (name: string) => {
    const { lock_timeout } = config;
    const results = await knex(name)
      .update({ status: 'completing-locked', modified: CURRENT_TIMESTAMP })
      .whereExists(knex('paused').select('*').where({ name, paused: false }))
      .andWhere('id', '=', knex(name)
        .select('id')
        .orderBy('created')
        .limit(1)
      )
      .andWhere(q => q
        .where({ status: 'processed' })
        .orWhere(q => q
          .whereRaw('EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - modified)) > ?', [ lock_timeout ])
          .andWhere({ status: 'completing-locked' })
        )
      )
      .returning(['payload', 'id']);

    if (results.length < 1) {
      return;
    }
    const id = get<number>(results, '0.id');
    const payload = JSON.parse(get<string>(results, '0.payload'));
    return { id, payload };
  };

  const get_job_to_process = async (name: string) => {
    const { lock_timeout, max_parallel } = config;
    const results = await knex(name)
      .update({ status: 'processing-locked', modified: CURRENT_TIMESTAMP })
      .whereExists(knex('paused').select('*').where({ name, paused: false }))
      .andWhere(q => q.whereIn('id', knex(name)
        .select('id')
        .where('ready', '<=', CURRENT_TIMESTAMP)
        .orderBy('created')
        .limit(max_parallel)
      ))
      .andWhere(q => q
        .where({ status: 'ready' })
        .orWhere(q => q
          .whereRaw('EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - modified)) > ?', [ lock_timeout ])
          .andWhere({ status: 'processing-locked' })
        )
      )
      .returning(['payload', 'id']);

    if (results.length < 1) {
      return;
    }
    const id = get<number>(results, '0.id');
    const payload = JSON.parse(get<string>(results, '0.payload'));
    return { id, payload };
  };

  const process_job = async (name: string, handler: Handler) => {
    const job = await get_job_to_process(name);
    if (!job) return;
    const { id, payload } = job;
    try {
      await handler(payload)
      await knex(name).where({ id }).update({ status: 'processed' });
    } catch (error) {
      console.error('Processing failed with error:', error);
      await knex(name).where({ id }).update({ status: 'error' });
    }
  };

  const complete_job = async (name: string, handler: Handler) => {
    const job = await get_job_to_complete(name);
    if (!job) return;
    const { id, payload } = job;
    try {
      await handler(payload);
      await knex(name).where({ id }).delete();
    } catch (error) {
      console.error('Completing failed with error:', error);
      await knex(name).where({ id }).update({ status: 'error' });
    }
  };

  const assert_queue = async (name: string, { paused }: { paused?: boolean} = {}) => {
    await knex.schema.createTableIfNotExists(name, table => {
      table.increments('id').primary();
      table.string('status').defaultTo('ready');
      table.json('payload');
      table.timestamp('created').defaultTo(CURRENT_TIMESTAMP);
      table.timestamp('modified').defaultTo(CURRENT_TIMESTAMP);
      table.timestamp('ready');
    });
    if (paused !== undefined) {
      await knex.raw(`
        INSERT INTO paused (name, paused)
        VALUES (:name, :paused)
        ON CONFLICT (name) DO UPDATE SET paused=:paused
      `, { name, paused });
    } else {
      await knex.raw(`
        INSERT INTO paused (name, paused)
        VALUES (:name, FALSE)
        ON CONFLICT (name) DO NOTHING
      `, { name });
    }

    return {
      pause: async () => {
        await knex.raw(`
          INSERT INTO paused (name, paused)
          VALUES (:name, 't')
          ON CONFLICT (name) DO UPDATE SET paused='t'
        `, { name });
      },
      resume: async () => {
        await knex.raw(`
          INSERT INTO paused (name, paused)
          VALUES (:name, 'f')
          ON CONFLICT (name) DO UPDATE SET paused='f'
        `, { name });
      },
      publish: async (data: any, options: PublishConfig = {}) => {
        const payload = JSON.stringify(data);
        const ready = `CURRENT_TIMESTAMP + INTERVAL '${Number(options.delay) || 0} milliseconds'`;
        await knex(name).insert({ payload, ready });
      },
      process: async (handler: Handler) => {
        const emitter = new EventEmitter();
        (async () => {
          while (true) {
            try {
              await process_job(name, handler);
              await sleep(config.poll_interval);
            } catch (error) {
              emitter.emit('error', error);
            }
          }
        })();
        return emitter;
      },
      complete: async (handler: Handler) => {
        const emitter = new EventEmitter();
        (async () => {
          while (true) {
            try {
              await complete_job(name, handler);
              await sleep(config.poll_interval);
            } catch (error) {
              emitter.emit('error', error);
            }
          }
        })();
        return emitter;
      }
    }
  };

  return {
    assert_queue
  }
}

