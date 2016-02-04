// Not sure how to run this as part of Mocha, since the test relies on
// whether or not all handles are appropriately cleaned up and allows
// Node to exit.
var memcached_bclient = require('../libs/memcached_bclient.js');
var mbc = new memcached_bclient('localhost:11211', {
  max_reconnect_tries: 0
});
mbc.set('foo', 'bar');
mbc.close();
console.log('Closed, this should gracefully exit Node');