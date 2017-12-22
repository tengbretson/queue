import { make_client } from './';
import * as cluster from 'cluster';

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

  var t = await queue.do_the_dew();
  console.log(t);

  // const processor = await queue.process(async payload => {
  //   await sleep(900); // simulate the part of the task that can be run concurrently
  //   console.log('processed', payload.number, 'on', process.pid);
  //   return;
  // });

  // processor.on('error', error => {
  //   console.error(error);
  // })
  
  // const sender = await queue.complete(async payload => {
  //   await sleep(200); // simulate the part of the task that can must be run sequentially
  //   console.log('sent', payload.number, 'on', process.pid);
  //   return;
  // });
  
  // sender.on('error', error => {
  //   console.error(error);
  // });
})();
