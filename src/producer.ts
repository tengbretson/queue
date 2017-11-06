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
  const queue = await client.assert_queue('my_queue');
  let t = 0;


  const enqueue: () => Promise<void> = async () => {
    await queue.enqueue({
      number: ++t
    });
    console.log('sent', t);
    await sleep(40);
    if (t < 10000) {
      return await enqueue();
    } else {
      return process.exit(0);
    }
  };

  await enqueue();
})();
