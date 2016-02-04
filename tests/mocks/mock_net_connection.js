/*
 * Copyright 2016, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

var _ = require('lodash');
var assert = require('assert');
var domain = require('domain');
var stream = require('stream');
var util = require('util');

var mock_domain = domain.create();

var caller_line_regex = /.*\n.*\n[^(]+\(([^)]+)\)/;

var MockConnection = function(server_or_port, host_if_port) {
  stream.Duplex.call(this, {});
  this.server = host_if_port ? [server_or_port, host_if_port] : server_or_port;
  this.operation_stack = [];
  this.readyState = null;
  this.pending_responses = 0;
};

util.inherits(MockConnection, require('stream').Duplex);

MockConnection.prototype.addWriteOp = function(write_buffer, callback_value, read_buffers, caller_line) {
  caller_line = caller_line || new Error().stack.match(caller_line_regex)[1];
  this.operation_stack.push({
    write_buffer: write_buffer,
    callback_value: callback_value,
    read_buffers: read_buffers ? (_.isArray(read_buffers) ? read_buffers : [read_buffers]) : null,
    caller_line: caller_line
  });
};

MockConnection.prototype.addReadOp = function(read_buffers, trigger_now, caller_line) {
  caller_line = caller_line || new Error().stack.match(caller_line_regex)[1];
  this.operation_stack.push({
    read_buffers: read_buffers ? (_.isArray(read_buffers) ? read_buffers : [read_buffers]) : null,
    caller_line: caller_line
  });
  if (trigger_now) {
    this.triggerEvents();
  }
};

MockConnection.prototype.addEventOp = function(event_type, event_data, trigger_now, caller_line) {
  caller_line = caller_line || new Error().stack.match(caller_line_regex)[1];
  this.operation_stack.push({
    event_type: event_type,
    event_data: event_data,
    caller_line: caller_line
  });
  if (trigger_now) {
    this.triggerEvents();
  }
};

MockConnection.prototype.addCallbackOp = function(callback, trigger_now, caller_line) {
  caller_line = caller_line || new Error().stack.match(caller_line_regex)[1];
  this.operation_stack.push({
    callback: callback,
    caller_line: caller_line
  });
  if (trigger_now) {
    this.triggerEvents();
  }
};

MockConnection.prototype.triggerEvents = function() {
  // Trigger events in the stack
  while (this.operation_stack.length && !this.operation_stack[this.operation_stack.length-1].write_buffer) {
    var op = this.operation_stack.pop();
    if (op.event_type) {
      this.emit(op.event_type, op.event_data);
    } else if (op.read_buffers) {
      var i;
      for (i = 0; i < op.read_buffers.length; i++) {
        this.push(op.read_buffers[i]);
      }
    }
    op.callback && op.callback();
  }
};

MockConnection.prototype.fireConnect = function() {
  this.addEventOp('connect');
  this.readyState = 'open';
  this.triggerEvents();
};

MockConnection.prototype.checkExpectations = function() {
  var stack_length = this.operation_stack.length;
  if (stack_length) {
    var caller_line = this.operation_stack[0].caller_line;
    this.operation_stack.length = 0;
    assert.ok(!stack_length, 'mock_net_connection had remaining output set at ' + caller_line);
  }
};

MockConnection.prototype._read = function() {
  // Don't do anything, we'll push when we see a request
};

MockConnection.prototype._write = function(chunk, encoding, callback) {
  assert.ok(this.operation_stack.length, 'mock_net_connection stack underflow');
  var op = this.operation_stack.pop();
  assert.equal(op.write_buffer.length, chunk.length, 'mock_net_connection write length mismatch:\nExp: ' + JSON.stringify(op.write_buffer) + '\nSaw: ' + JSON.stringify(chunk));
  var i;
  for (i = 0; i < op.write_buffer.length; i++) {
    if (op.write_buffer[i] !== chunk[i]) {
      assert.equal(op.write_buffer[i], chunk[i], 'mock_net_connection write value mismatch at ' + i + ':\nExp: ' + op.write_buffer[i] +' in ' + JSON.stringify(op.write_buffer) + '\nSaw: ' + chunk[i] + ' in ' + JSON.stringify(chunk));
    }
  }
  callback(op.callback_value);

  var self = this;
  function response() {
    self.pending_responses -= 1;
    if (op.read_buffers) {
      for (i = 0; i < op.read_buffers.length; i++) {
        self.push(op.read_buffers[i]);
      }
    }
    self.triggerEvents();
  }
  // Run this in a different domain than the caller, since our actual memcache
  // module will run the response in the domain which created the connection,
  // and bound the packet handler, not the one that is writing to the
  // connection.
  setTimeout(mock_domain.run.bind(mock_domain, response), 1);
  self.pending_responses += 1;
};

MockConnection.prototype.setTimeout = function() {};
MockConnection.prototype.setNoDelay = function() {};


var MockNet = function() {
  this.connections = [];
};

MockNet.prototype.createConnection = function(server_or_port, host_if_port) {
  var connection = new MockConnection(server_or_port, host_if_port);
  this.connections.push(connection);
  return connection;
};

MockNet.prototype.lastConnection = function() {
  return this.connections[this.connections.length - 1];
};

MockNet.prototype.fireConnect = function() {
  this.lastConnection().fireConnect();
};

MockNet.prototype.addWriteOp = function(write_buffer, callback_value, read_buffers) {
  this.lastConnection().addWriteOp(write_buffer, callback_value, read_buffers, new Error().stack.match(caller_line_regex)[1]);
};

MockNet.prototype.addReadOp = function(read_buffers, trigger_now) {
  this.lastConnection().addReadOp(read_buffers, trigger_now, new Error().stack.match(caller_line_regex)[1]);
};

MockNet.prototype.addEventOp = function(event_type, event_data, trigger_now) {
  this.lastConnection().addEventOp(event_type, event_data, trigger_now, new Error().stack.match(caller_line_regex)[1]);
};

MockNet.prototype.addCallbackOp = function(callback, trigger_now) {
  this.lastConnection().addCallbackOp(callback, trigger_now, new Error().stack.match(caller_line_regex)[1]);
};

MockNet.prototype.triggerEvents = function() {
  this.lastConnection().triggerEvents();
};


MockNet.prototype.checkExpectations = function() {
  var i;
  for (i = 0; i < this.connections.length; i++) {
    this.connections[i].checkExpectations();
  }
  this.connections.length = 0;
};

module.exports = MockNet;
