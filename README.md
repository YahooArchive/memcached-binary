**Please Note**: This repo has a known security vulnerability and is no longer maintained. Use at your own risk!

memcached-binary
================

Binary Memcached client for Node.js

Compared to other Node.js memcache clients, this uses the Memcache binary API exclusively, which means you are able to store and retrieve any data, including those which are raw binary or contain newlines, which cause problems with other implementations.

Usage:
```
var MemcachedBinary = require('memcached-binary');

var server = 'localhost:11211'; // '/somesocketpath' or 'somehost:someport'
var params = { // Various params and options
  use_buffers: false, // If true, always return Buffers instead of strings; defaults to false
};
var memcached_binary = new MemcachedBinary(server, params);

memcached_binary.set('key', 'value');
memcached_binary.get('key', null, function(err, res) {
  console.log(err || res); // Will log 'value'
});
```

[Auto-generated API docs](https://yahoo.github.io/memcached-binary/docs/classes/MemcachedBinaryClient.html)

Code licensed under the New BSD license. See LICENSE file for terms.
