"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
var _1 = require("./");
var config = {
    poll_interval: 100,
    lock_timeout: 30,
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
_1.make_client(config).then((client) => {
    var t = 0;
    setInterval(function () {
        client.publish('my_queue', {
            number: ++t
        }, {});
    }, 2020);

});
