import { make_client } from './';

const config = {
    lock_timeout: 30,
    poll_interval: 100,
    pool: {
        user: 'root',
        password: 'password',
        database: 'queue',
        host: '192.168.1.1',
        port: 5433,
        max: 20,
        idleTimeoutMillis: 30000,
        connectionTimeoutMillis: 2000
    }
};


make_client(config).then(client => {
  return client.make_queue('my_queue').then(() => {
    return client.subscribe('my_queue', function (payload, ack, nack) {
        console.log(payload);
        ack();
    });
  }, e => console.error('pp', e));
}, e => console.error('l', e)).catch(err => console.error(err))
