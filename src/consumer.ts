import { make_client } from './'
const config = {
  user: 'root',
  password: 'password',
  database: 'queue',
  host: '192.168.1.1',
  port: 5433,
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000
};

const client = make_client(config);

client.subscribe('my_queue', (payload, ack, nack) => {
  console.log('receiving job', payload.number);
  setTimeout(() => {
    console.log('acking job');
    ack();
  }, 1000)
});