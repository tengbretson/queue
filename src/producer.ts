import { make_client } from './';

const config = {
  lock_timeout: 30,
  poll_interval: 100,
  max_parallel: 10,
  empty_backoff: 100,
  pool: {
    min: 5,
    max: 10
  },
  connection: {
    user: process.env.PG_USER || 'root',
    password: process.env.PG_PASS || 'password',
    database: process.env.PG_DATABASE || 'queue',
    host: process.env.PG_HOST || '192.168.1.1',
    port: process.env.PG_PORT || 5433
  }
};

const sleep = (t: number) => new Promise(resolve => setTimeout(resolve, t));

(async () => {
  const client = await make_client(config);
  const queue = await client.assert_queue(13);
  const i = 37;
  // for (let i = 0; i < 10000; i++) {
    await queue.enqueue({ number: i });
    console.log('sent', i);
    await sleep(40);
  // }
})();
