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

if (cluster.isMaster) {
  let start_time: number;
  console.log(`Master ${process.pid} is running`);

  (async () => {
    for (let i = 0; i < 12; i++) {
      var worker = cluster.fork();
      worker.on('message', (msg) => {
        if (msg == 1) {
          start_time = Date.now();
        }
        if (msg == 10000) {
          console.log(Date.now() - start_time);
          process.exit(0);
        }
      });
      await sleep(100);
    }

    cluster.on('exit', (worker, code, signal) => {
      console.log(`worker ${worker.process.pid} died`);
    });

  })()

} else {
  (async () => {
    const client = await make_client(config);
    const queue = await client.assert_queue('my_queue');
  
    const processor = await queue.process(async payload => {
      console.log('processing', payload.number, 'on', process.pid);
      await sleep(900);
      console.log('processed', payload.number, 'on', process.pid);
      return;
    });
  
    processor.on('error', error => {
      console.error(error);
    })
    
    const sender = await queue.complete(async payload => {
      console.log('sending', payload.number, 'on', process.pid);
      await sleep(200);
      console.log('sent', payload.number, 'on', process.pid);
      if (process.send) {
        process.send(payload.number);
      }
      return;
    });
    
    sender.on('error', error => {
      console.error(error);
    });
  })();
}
