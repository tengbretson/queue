import { make_client } from './';

const config = {
  lock_timeout: 30,
  poll_interval: 100,
  max_parallel: 10,
  pool: {
    min: 5,
    max: 10
  },
  connection: {
    user: 'root',
    password: 'password',
    database: 'queue',
    host: '192.168.1.1:5433'
  }
};

(async () => {
  
})();

make_client(config).then(client => {


  return client.make_queue('my_queue')
    .then(() => {
        return client.subscribe('my_queue', payload => {
            console.log(payload)
            return Promise.resolve();
        });
    }, e => console.error('pp', e));
}, e => console.error('l', e)).catch(err => console.error(err))
