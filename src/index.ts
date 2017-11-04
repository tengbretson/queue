import { get } from 'lodash';
import * as Knex from 'knex';
import { EventEmitter } from 'events';

export interface Handler { (payload: any): Promise<void> };

export interface QueueConfig {
  pool: Knex.PoolConfig;
  poll_interval: number;
  lock_timeout: number;
  connection: Knex.ConnectionConfig;
};

export interface PublishConfig {
  delay?: number;
}


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

  const get_ready_job = async (name: string) => {
    const { lock_timeout } = config;
    const results = await knex(name)
      .update({ status: 'locked', modified: CURRENT_TIMESTAMP })
      .whereExists(knex('paused').select('*').where({ name, paused: false }))
      .andWhere('id', '=', knex(name)
        .select('id')
        .where('ready', '<=', CURRENT_TIMESTAMP)
        .orderBy('created')
        .limit(1)
      )
      .andWhere(q => q
        .where({ status: 'ready' })
        .orWhere(q => q
          .whereRaw('EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - modified)) > ?', [ lock_timeout ])
          .andWhere({ status: 'locked' })
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

  const make_ack = (name: string, id: number) => async () => {
    try {
      await knex(name).where({ id }).delete();
    } catch (error) {
      console.error(error);
    }
  };

  const make_nack = (name: string, id: number) => async () => {
    try {
      await knex(name).where({ id }).update({ status: 'ready' });
    } catch (error) {
      console.error(error);
    }
  };

  const process_job = async (queue_name: string, handler: Handler) => {
    try {
      const job = await get_ready_job(queue_name);
      if (!job) return;
      await handler(job.payload)
        .then(make_ack(queue_name, job.id), make_nack(queue_name, job.id));
    } catch (error) {
      console.error(error);
    }
  };

  const assert_queue = async (name: string) => {
    await knex.schema.createTableIfNotExists(name, table => {
      table.increments('id').primary();
      table.string('status').defaultTo('ready');
      table.json('payload');
      table.timestamp('created').defaultTo(CURRENT_TIMESTAMP);
      table.timestamp('modified').defaultTo(CURRENT_TIMESTAMP);
      table.timestamp('ready');
    });
  };

  const sleep = (timeout: number) => new Promise(resolve => setTimeout(resolve, timeout));

  return {
    make_queue: async (name: string, options: { paused?: boolean} = {}) => {
      await assert_queue(name);
      if (options.paused !== undefined) {
        await knex.raw(`
          INSERT INTO paused (name, paused)
          VALUES (:name, :paused)
          ON CONFLICT (name) DO UPDATE SET paused=:paused
        `, { name, paused: options.paused });
      }
    },
    pause: async (name: string) => {
      await knex.raw(`
        INSERT INTO paused (name, paused)
        VALUES (:name, 't')
        ON CONFLICT (name) DO UPDATE SET paused='t'
      `, { name });
    },
    resume: async (name: string) => {
      await knex.raw(`
        INSERT INTO paused (name, paused)
        VALUES (${name}, 'f')
        ON CONFLICT (name) DO UPDATE SET paused='f'
      `, { name });
    },
    publish: async (name: string, data: any, options: PublishConfig = {}) => {
      const payload = JSON.stringify(data);
      const ready = `CURRENT_TIMESTAMP + INTERVAL '${Number(options.delay) || 0} milliseconds'`;
      await assert_queue(name);
      await knex(name).insert({ payload, ready });
    },
    subscribe: async (queue_name: string, handler: Handler) => {
      await assert_queue(queue_name);
      const emitter = new EventEmitter();
      (async () => {
        while (true) {
          try {
            await process_job(queue_name, handler);
            await sleep(config.poll_interval);
          } catch (error) {
            emitter.emit('error', error);
          }
        }
      })();
      return emitter;
    }
  }
}

