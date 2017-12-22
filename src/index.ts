import { get, range } from 'lodash';
import * as Knex from 'knex';
import { EventEmitter } from 'events';
import { v4 as uuid } from 'uuid';

export interface Handler { (payload: any): Promise<void> };

export interface QueueConfig {
  pool: Knex.PoolConfig;
  poll_interval: number;
  lock_timeout: number;
  max_parallel: number;
  empty_backoff: number;
  connection: Knex.ConnectionConfig;
};

export interface PublishConfig {
  delay?: number;
}

const sleep = (t: number) => new Promise(resolve => setTimeout(resolve, t));

const initialize_paused = async (knex: Knex) => {
  if (!await knex.schema.hasTable('paused')) {
    await knex.schema.createTableIfNotExists('paused', table => {
      table.integer('queue_id').notNullable().primary();
      table.boolean('paused');
    });
  }
};

const initialize_queue = async (knex: Knex) => {
  if (!await knex.schema.hasTable('queue')) {
    const CURRENT_TIMESTAMP = knex.raw('CURRENT_TIMESTAMP');
    await knex.schema.createTableIfNotExists('queue', table => {
      table.bigIncrements('id').primary();
      table.integer('queue_id').references('queue_id').inTable('paused').notNullable();
      table.string('status').notNullable().defaultTo('ready');
      table.json('concurrent_payload');
      table.json('sequential_payload');
      table.timestamp('queued_at').notNullable().defaultTo(CURRENT_TIMESTAMP);
      table.timestamp('locked_at');
    });
  }
};

export const make_client = async (config: QueueConfig) => {
  const { pool, connection, lock_timeout } = config;
  const knex = Knex({ client: 'pg', pool, connection });

  const concurrent_processors = new Map<string, Handler>();
  const sequential_processors = new Map<string, Handler>();

  await initialize_paused(knex);
  await initialize_queue(knex);
  


  const get_job = async (last_queue = 0) => {
    const { rows } = await knex.raw(`
    UPDATE queue
    SET
      locked_at = NOW(),
      status = CASE WHEN (status = 'ready' OR status = 'locked-concurrent')
        THEN 'locked-concurrent'
        ELSE 'locked-sequential'
      END
    WHERE queue.id = (
      SELECT queue.id FROM queue INNER JOIN paused ON queue.queue_id = paused.queue_id
      WHERE paused.paused = false
      AND queue.queue_id > :last_queue
      AND (
        queue.status = 'ready' OR (
          queue.status = 'locked-concurrent'
            AND EXTRACT(
              EPOCH FROM (NOW() - queue.locked_at)
            ) > :lock_timeout
          ) OR (
            queue.id IN (select distinct on (queue_id) id FROM queue ORDER BY queue_id, queued_at ASC, id)
              AND queue.status = 'processed-concurrent' OR (
                queue.status = 'locked-sequential'
                  AND EXTRACT(
                    EPOCH FROM (NOW() - queue.locked_at)
                  ) > :lock_timeout
              )
          )
        )
      ORDER BY queue.queue_id, queue.queued_at
      LIMIT 1
      FOR UPDATE
    )
    RETURNING
      id,
      queue_id,
      status,
      CASE WHEN (status = 'locked-concurrent')
        THEN concurrent_payload
        ELSE sequential_payload
      END as payload
    `, { lock_timeout, last_queue });
    if (rows.length < 1) {
      return;
    }
    const queue_id: number = get(rows, '0.queue_id');
    const status: number = get(rows, '0.status');
    const id: number = get(rows, '0.id');
    const payload: any = get(rows, '0.payload');
    return { id, status, queue_id, payload };
  };


  const register_concurrent = (queue_name: string, handler: Handler) =>
    concurrent_processors.set(queue_name, handler);

  const register_sequential = (queue_name: string, handler: Handler) =>
    sequential_processors.set(queue_name, handler);

  const assert_queue = async (queue_id: number, { paused }: { paused?: boolean} = {}) => {
    if (paused !== undefined) {
      await knex.raw(`
        INSERT INTO paused (queue_id, paused)
        VALUES (:queue_id, :paused)
        ON CONFLICT (queue_id) DO UPDATE SET paused=:paused
      `, { queue_id, paused });
    } else {
      await knex.raw(`
        INSERT INTO paused (queue_id, paused)
        VALUES (:queue_id, FALSE)
        ON CONFLICT (queue_id) DO NOTHING
      `, { queue_id });
    }

    return {
      pause: async () => {
        await knex.raw(`
          INSERT INTO paused (queue_id, paused)
          VALUES (:queue_id, 't')
          ON CONFLICT (queue_id) DO UPDATE SET paused='t'
        `, { queue_id });
      },
      resume: async () => {
        await knex.raw(`
          INSERT INTO paused (queue_id, paused)
          VALUES (:queue_id, 'f')
          ON CONFLICT (queue_id) DO UPDATE SET paused='f'
        `, { queue_id });
      },
      enqueue: async (data: any, options: PublishConfig = {}) => {
        const concurrent_payload = JSON.stringify(data);
        return await knex('queue').insert({ queue_id, concurrent_payload });
      },
      do_the_dew: () => get_job(0)
      // process: async (handler: Handler) => {
      //   const emitter = new EventEmitter();
      //   (async () => {
      //     while (true) {
      //       try {
      //         await process_job(name, handler);
      //       } catch (error) {
      //         emitter.emit('error', error);
      //       }
      //     }
      //   })();
      //   return emitter;
      // },
      // complete: async (handler: Handler) => {
      //   const emitter = new EventEmitter();
      //   (async () => {
      //     while (true) {
      //       try {
      //         await complete_job(name, handler);
      //       } catch (error) {
      //         emitter.emit('error', error);
      //       }
      //     }
      //   })();
      //   return emitter;
      // }
    }
  };

  return {
    register_concurrent,
    register_sequential,
    assert_queue
  }
}

