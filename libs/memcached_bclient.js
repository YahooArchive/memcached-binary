/*
 * Copyright (c) 2011 Tim Eggert
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @author Tim Eggert <tim@elbart.com>
 * @license http://www.opensource.org/licenses/mit-license.html MIT License
 *
 */

/*
 * This is a heavily modified version of https://github.com/elbart/node-memcache/
 * which is Copyright 2011 Tim Eggert per the above copyright notice.
 * Most portions of this code are Copyright 2016, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

// Simple Memcached client connection using the binary protocol
// Default operation mode still ends up reading and writing strings, but the
// binary protocol is faster and less error-prone.

var net = require('net');
var util = require('util');
var assert = require('assert');

/**
 * Memcached binary client
 *
 * @class MemcachedBinaryClient
 * @param {String} server The memcached server location (e.g. '/somesocketpath' or 'somehost:someport')
 * @param {Object} [params] Optional params object
 * @param {Object} [params.logger] Console-compatible logger, defaults to console
 * @param {Integer} [params.max_reconnect_tries] Maximum number of times to try
 *                  reconnecting after a disconnect, set to 0 to disable automatic
 *                  reconnects, default = Infinity
 * @param {Object} [params.use_buffers] Return Buffer objects instead of strings,
 *                  default = false
 * @constructor
 */
var MemcachedBinaryClient = function(server, params) {
  params = params || {};
  this.server = server || [11211, 'localhost'];
  if (typeof this.server === 'string' && this.server[0] !== '/') {
    // A string like localhost:11211, need to split into [port, host]
    // String paths that start with '/' we leave alone, as they're unix socket paths
    this.server = this.server.split(':').reverse();
  }
  this.connection = null;
  this.logger = params.logger || console;

  this.requests = []; // FIFO of outstanding request objects

  this.response_data_buffer = null; // Binary buffer for incomplete binary responses
  this.request_buffer_pool = []; // Pool of finished request buffers

  this.status_messages = []; // Maps status codes to their error messages so we don't rebuild them every time

  this.max_reconnect_tries = typeof params.max_reconnect_tries === 'number' ? params.max_reconnect_tries : Infinity;
  this.connect_tries = 0; // How many times we've tried to connect
  this.connect_timeout = null; // The timeout for next connect attempt

  this.use_buffers = !!params.use_buffers;

  this.connect();
};

util.inherits(MemcachedBinaryClient, process.EventEmitter);

/**
 * Connect to the memcached server. Called automatically by the constructor.
 * @method connect
 */
MemcachedBinaryClient.prototype.connect = function () {
  var self = this;

  if (self.connect_timeout) {
    clearTimeout(self.connect_timeout);
    self.connect_timeout = null;
  }

  if (!self.connection) {
    self.connect_tries += 1;

    if (Array.isArray(self.server)) {
      self.connection = net.createConnection(self.server[0], self.server[1]);
    } else {
      self.connection = net.createConnection(self.server);
    }

    this.connection.addListener('connect', function () {
      self.logger.log('Memcached CONNECT: ' + self.server + ' ' + (self.connection ? self.connection.readyState : 'disconnected'));
      this.setTimeout(0);
      this.setNoDelay();
      self.emit('connect');
      self.connect_tries = 0;
    });

    this.connection.addListener('data', function (data) {
      if (self.response_data_buffer) {
        var newdata = new Buffer(self.response_data_buffer.length + data.length);
        self.response_data_buffer.copy(newdata);
        data.copy(newdata, self.response_data_buffer.length);
        data = newdata;
        self.response_data_buffer = null;
      }

      while(data) {
        data = self.handleData(data);
      }
    });

    this.connection.addListener('end', function () {
      self.logger.log('Memcached END');
      self.connection && self.connection.end();
      self.removeConnection();
    });

    this.connection.addListener('close', function () {
      self.logger.log('Memcached CLOSE');
      self.removeConnection();
      self.emit('close');
    });

    this.connection.addListener('timeout', function () {
      self.logger.log('Memcached TIMEOUT');
      self.removeConnection();
      self.emit('timeout');
    });

    this.connection.addListener('error', function (ex) {
      self.logger.log('Memcached ERROR: ' + JSON.stringify(ex));
      self.removeConnection();
      // Fail less catastrophically than error
      self.emit('close');
    });
  }
};

/**
 * Returns true if the internal connection is open
 * @method isConnected
 */
MemcachedBinaryClient.prototype.isConnected = function() {
  return this.connection ? this.connection.readyState === 'open' : false;
};

/**
 * Closes the connection to the memcached server
 * @method close
 */
MemcachedBinaryClient.prototype.close = function() {
  if (this.connection) {
    this.logger.log('Memcached closing');
    this.connection.end();
    this.removeConnection();
  }
};

// Clean up after any kind of disconnect
// Removes the connection, clears the data buffer, and sends all outstanding requests error responses, may automatically call reconnect
MemcachedBinaryClient.prototype.removeConnection = function() {
  if (this.connection) {
    this.logger.log('Memcached removing connection, ' + this.requests.length + ' outstanding');
    this.connection = null;
    this.response_data_buffer = null;
    while(this.requests.length) {
      var request = this.requests.shift();
      if (request !== null && request.callback){
        request.callback('Memcached connection removed', null);
      }
    }

    if (this.connect_tries >= this.max_reconnect_tries) {
      // have tried enough times, permanent failure
    } else {
      this.reconnect();
    }
  }
};

// Sets a timeout to connect() based on number of prior connect attempts
MemcachedBinaryClient.prototype.reconnect = function() {
  if (!this.connection && !this.connect_timeout) {
    var backoff = Math.pow(2, this.connect_tries > 8 ? 8 : this.connect_tries);
    this.logger.log('Memcached reconnecting in ' + backoff + 's');
    this.connect_timeout = setTimeout(this.connect.bind(this), 1000 * backoff);
  }
};


// Binary interface

// Finds the smallest buffer in the bucket that is at least len
MemcachedBinaryClient.prototype.getRequestBuffer = function(len) {
  var i, buf, bufidx = -1, buflen = 0;
  for (i = this.request_buffer_pool.length - 1; i >= 0; i--) {
    if (this.request_buffer_pool[i].length >= len && (!buflen || this.request_buffer_pool[i].length < buflen)) {
      buflen = this.request_buffer_pool[i].length;
      bufidx = i;
    }
  }
  if (bufidx >= 0) {
    buf = (this.request_buffer_pool.splice(bufidx, 1))[0];
  } else {
    buf = new Buffer(len);
    buf[0] = 0x80;
  }
  assert.ok(buf[0] === 0x80);
  buf.fill(0, 2, 24);
  return buf;
};

// Processes incoming data from the server
MemcachedBinaryClient.prototype.handleData = function(buf) {
  assert.ok(buf[0] === 0x81);

  if (buf.length < 24) {
    // Don't have a full header yet, save it for later
    this.response_data_buffer = buf;
    return null;
  }

  var opcode = buf[1];
  var status = buf[7]; // No status values use byte 6 yet
  var body_length = (buf[8] << 24) + (buf[9] << 16) + (buf[10] << 8) + buf[11];

  if (buf.length < 24 + body_length) {
    // Don't have the full body yet, save it for later
    this.response_data_buffer = buf;
    return null;
  }

  var result_error = null, result_value = null, result_buf;
  if (status) {
    if (!this.status_messages[status]) {
      // errors have no extras/key
      this.status_messages[status] = buf.toString('utf8', 24, 24 + body_length);
    }
    if (status !== 1) { // 1 (Not found) is not an error
      result_error = status.toString() + ' ' + (this.status_messages[status] || 'Unknown error');
    }
  } else {
    var key_length = (buf[2] << 8) + buf[3];
    var extras_length = buf[4];
    if (this.use_buffers) {
      result_value = buf.slice(24 + extras_length + key_length, 24 + body_length);
    } else {
      result_value = buf.toString('utf8', 24 + extras_length + key_length, 24 + body_length);
    }
  }

  var request = this.requests.shift();
  if (!request) {
    // Got a response and we don't have any requests?!
    this.logger.error('Memcached null request, ' + this.requests.length + ' outstanding');
    this.close();
    return null;
  }
  if (request.opcode !== opcode) {
    // Got a mismatched response
    this.logger.error('Memcached request response opcode mismatch: request ' + request.opcode + ' response ' + opcode);
    this.requests.unshift(request); // Put it back into the request list so its callback is called when we close
    this.close();
    return null;
  }

  if (request.callback){
    if (request.cas_request) {
      result_value = {
        val: result_value,
        cas: {
          high: (buf[16] << 24) + (buf[17] << 16) + (buf[18] << 8) + buf[19],
          low: (buf[20] << 24) + (buf[21] << 16) + (buf[22] << 8) + buf[23]
        }
      };
    }
    request.callback(result_error, result_value);
  }

  if (buf.length > 24 + body_length) {
    result_buf = buf.slice(24 + body_length);
  }

  return result_buf;
};

// Makes a request to the server
MemcachedBinaryClient.prototype.bquery = function(buffer, length, opcode, cas_request, callback) {
  var self = this;
  if (self.connection) {
    assert.ok(buffer[0] === 0x80);
    var request_domain = process.domain;
    if (request_domain && callback) {
      var orig_callback = callback;
      callback = function (err, result) {
        request_domain.run(function () {
          orig_callback(err, result);
        });
      };
    }
    self.requests.push({ opcode: opcode, cas_request: cas_request, callback: callback });
    var did_pool = false;
    self.connection.write(buffer.slice(0, length), function() {
      assert.ok(!did_pool);
      did_pool = true;
      self.request_buffer_pool.push(buffer);
    });
  } else if (callback) {
    callback('Memcached not connected', null);
  }
};

// Makes a general store-type request to the server (e.g. set, add)
MemcachedBinaryClient.prototype.bstore = function(opcode, key, value, lifetime, flags, cas_test, callback) {
  var set_flags = flags || 0;
  var exp_time  = lifetime || 0;
  var value_is_buffer = Buffer.isBuffer(value);
  var value_str;
  var value_bytes;
  if (!value_is_buffer) {
    value_str = value.toString();
    value_bytes = Buffer.byteLength(value_str);
  } else {
    value_bytes = value.length;
  }
  var extra_len = 8; // set, add and replace all include 4 byte flags and 4 byte expiration
  var buflen = 24 + extra_len + Buffer.byteLength(key) + value_bytes;
  var buf = this.getRequestBuffer(buflen);
  buf[1] = opcode;
  buf[4] = extra_len;
  if (cas_test) {
    buf.writeUInt32BE(cas_test.high, 16);
    buf.writeUInt32BE(cas_test.low, 20);
  }
  buf.writeUInt32BE(set_flags, 24);
  buf.writeUInt32BE(exp_time, 28);
  var key_bytes = buf.write(key, 32);
  if (value_is_buffer) {
    value.copy(buf, 32 + key_bytes);
  } else {
    var value_bytes_test = buf.write(value_str, 32 + key_bytes);
    assert.equal(value_bytes_test, value_bytes);
  }
  buf.writeUInt16BE(key_bytes, 2);
  buf.writeUInt32BE(extra_len + key_bytes + value_bytes, 8);

  var total_len = 24 + extra_len + key_bytes + value_bytes;
  assert.ok(total_len === buflen);
  return this.bquery(buf, total_len, opcode, false, callback);
};

// Makes a simple key-based request to the server with no extra parameters (e.g. get, del)
MemcachedBinaryClient.prototype.bkey = function(opcode, key, cas_request, callback) {
  var buflen = 24 + key.length;
  var buf = this.getRequestBuffer(buflen);
  buf[1] = opcode;
  var key_bytes = buf.write(key, 24);
  buf.writeUInt16BE(key_bytes, 2);
  buf.writeUInt32BE(key_bytes, 8);
  return this.bquery(buf, 24 + key_bytes, opcode, cas_request, callback);
};

/**
 * Retrieve data
 * @method get
 * @param {String} key Cache key
 * @param {Object} [params] Optional parameters (pass null if passing a callback argument)
 * @param {Boolean} [params.cas] Request a result object with value and cas fields
 * @param {Function} [callback] Function(err, res) called with memcached get err and result
 * @async
 */
MemcachedBinaryClient.prototype.get = function(key, params, callback) {
  params = params || {};
  return this.bkey(0x00, key, params.cas, callback);
};

/**
 * Store data
 * @method set
 * @param {String} key Cache key
 * @param {String} value Cached value
 * @param {Object} [params] Optional parameters (pass null if passing a callback argument)
 * @param {Number} [params.lifetime] Time in seconds to cache the results
 * @param {Object} [params.cas] Object with cas data to test against
 * @param {Number} [params.flags] Flags to set in memcached
 * @param {Function} [callback] Function(err, res) called with memcached set err and result
 * @async
 */
MemcachedBinaryClient.prototype.set = function(key, value, params, callback) {
  params = params || {};
  return this.bstore(0x01, key, value, params.lifetime, params.flags, params.cas, callback);
};

/**
 * Store data, fails if key exists
 * @method add
 * @param {String} key Cache key
 * @param {String} value Cached value
 * @param {Object} [params] Optional parameters (pass null if passing a callback argument)
 * @param {Number} [params.lifetime] Time in seconds to cache the results
 * @param {Number} [params.flags] Flags to set in memcached
 * @param {Function} [callback] Function(err, res) called with memcached add err and result
 * @async
 */
MemcachedBinaryClient.prototype.add = function(key, value, params, callback) {
  params = params || {};
  return this.bstore(0x02, key, value, params.lifetime, params.flags, null, callback);
};

/**
 * Store data, fails if the key doesn't exist
 * @method replace
 * @param {String} key Cache key
 * @param {String} value Cached value
 * @param {Object} [params] Optional parameters (pass null if passing a callback argument)
 * @param {Number} [params.lifetime] Time in seconds to cache the results
 * @param {Object} [params.cas] Object with cas data to test against
 * @param {Number} [params.flags] Flags to set in memcached
 * @param {Function} [callback] Function(err, res) called with memcached replace err and result
 * @async
 */
MemcachedBinaryClient.prototype.replace = function(key, value, params, callback) {
  params = params || {};
  return this.bstore(0x03, key, value, params.lifetime, params.flags, params.cas, callback);
};

/**
 * Remove data
 * @method del
 * @param {String} key Cache key
 * @param {Object} [params] Optional parameters (pass null if passing a callback argument)
 * @param {Boolean} [params.cas_request] Request a result object with value and cas fields, rather
 * @param {Function} [callback] Function(err, res) called with memcached del err and result
 * @async
 */
MemcachedBinaryClient.prototype.del = function(key, params, callback) {
  params = params || {};
  return this.bkey(0x04, key, null, callback);
};

module.exports = MemcachedBinaryClient;
