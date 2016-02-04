/*
 * Copyright 2016, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */
/*jshint node:true, unused:vars*/
/*global describe:true,it:true,before:true,after:true,afterEach:true */
"use strict";

var expect = require('chai').expect;
var mockery = require('mockery');

var buf_get_abc = new Buffer([128,0,0,3,0,0,0,0,0,0,0,3,0,0,0,0,0,0,0,0,0,0,0,0,97,98,99]);
var buf_set_abc_def = new Buffer([128,1,0,3,8,0,0,0,0,0,0,14,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,97,98,99,100,101,102]);
var buf_set_resp = new Buffer([129,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1]);
var server_string = '11211,localhost';

describe('#MemcachedBinaryClient', function () {
  var mock_net = new (require('../mocks/mock_net_connection.js'))();
  var mock_logger = new (require('../mocks/mock_logger.js'))();

  function logExpectConnect() {
    mock_logger.expect('log', 'Memcached CONNECT: 11211,localhost open');
  }

  function logExpectReconnect() {
    mock_logger.expect('log', 'Memcached reconnecting in 1s');
  }

  function logExpectClose(outstanding) {
    outstanding = outstanding || 0;
    mock_logger.expect('log', 'Memcached closing');
    mock_logger.expect('log', 'Memcached removing connection, ' + outstanding + ' outstanding');
  }

  function logExpectEvent(msg) {
    mock_logger.expect('log', msg);
    mock_logger.expect('log', 'Memcached removing connection');
  }

  function logExpectErrorClose(msg, outstanding) {
    mock_logger.expect('error', msg);
    logExpectClose(outstanding);
  }

  function mbcCreate(auto_reconnect, use_buffers) {
    logExpectConnect();
    var memcached_bclient = require('../../libs/memcached_bclient.js');
    var mbc = new memcached_bclient(undefined, {
      logger: mock_logger,
      use_buffers: use_buffers,
      max_reconnect_tries: (auto_reconnect === true) ? Infinity : (auto_reconnect || 0)
    });
    mock_net.fireConnect();
    return mbc;
  }

  before(function () {
    mockery.registerMock('net', mock_net);
    mockery.enable({
      useCleanCache: true,
      warnOnUnregistered: false
    });
    mockery.resetCache();
  });

  afterEach(function () {
    mock_net.checkExpectations();
    mock_logger.checkExpectations();
  });

  after(function () {
    mockery.disable();
    mockery.deregisterAll();
  });

  describe('#connection', function() {
    it('should create default server with default logger', function () {
      var memcached_bclient = require('../../libs/memcached_bclient.js');
      var mbc = new memcached_bclient();
      expect(mbc.server.toString()).to.equal('11211,localhost');
      expect(mbc.logger).to.equal(console);
    });

    it('should create default server', function () {
      var mbc = mbcCreate();
      expect(mbc.server.toString()).to.equal('11211,localhost');
      expect(mbc.isConnected()).to.equal(true);
    });

    it('should create specific host:port server', function () {
      mock_logger.expect('log', 'Memcached CONNECT: 11212,localhost open');
      var memcached_bclient = require('../../libs/memcached_bclient.js');
      var mbc = new memcached_bclient('localhost:11212', {logger: mock_logger});
      mock_net.fireConnect();
      expect(mbc.server.toString()).to.equal('11212,localhost');
      expect(mbc.isConnected()).to.equal(true);
    });

    it('should create specific socket server', function () {
      mock_logger.expect('log', 'Memcached CONNECT: /mock open');
      var memcached_bclient = require('../../libs/memcached_bclient.js');
      var mbc = new memcached_bclient('/mock', {logger: mock_logger});
      mock_net.fireConnect();
      expect(mbc.server.toString()).to.equal('/mock');
      expect(mbc.isConnected()).to.equal(true);
    });

    it ('should not reconnect an open connection', function() {
      var mbc = mbcCreate();
      var mbc_connection = mbc.connection;
      mbc.connect();
      expect(mbc.isConnected()).to.equal(true);
      expect(mbc_connection).to.equal(mbc.connection);
    });

    it ('should reconnect a closed connection', function() {
      var mbc = mbcCreate();
      var mbc_connection = mbc.connection;
      logExpectClose();
      mbc.close();
      logExpectConnect();
      mbc.connect();
      mock_net.fireConnect();
      expect(mbc.server.toString()).to.equal(server_string);
      expect(mbc.isConnected()).to.equal(true);
      expect(mbc_connection).to.not.equal(mbc.connection);
    });

    it('should close an open connection', function() {
      var mbc = mbcCreate();
      logExpectClose();
      mbc.close();
      expect(mbc.isConnected()).to.equal(false);
      expect(mbc.connection).to.equal(null);
    });

    it('should not reclose a closed connection', function() {
      var mbc = mbcCreate();
      logExpectClose();
      mbc.close();
      mbc.close();
      expect(mbc.isConnected()).to.equal(false);
      expect(mbc.connection).to.equal(null);
    });

    it('should not reremove a removed connection', function() {
      var mbc = mbcCreate();
      logExpectClose();
      mbc.close();
      mbc.removeConnection();
      expect(mbc.isConnected()).to.equal(false);
      expect(mbc.connection).to.equal(null);
    });

    it('should not callback with error to requests with no connection', function(done) {
      var mbc = mbcCreate();
      logExpectClose();
      mbc.close();
      mbc.set('abc', 'def');
      mbc.set('abc', 'def', undefined, function(err, res) {
        expect(mbc.isConnected()).to.equal(false);
        expect(err).to.equal('Memcached not connected');
        expect(res).to.equal(null);
        done();
      });
    });

    it('should auto reconnect a closed connection', function() {
      var mbc = mbcCreate(true);
      logExpectReconnect();
      logExpectClose();
      mbc.close();
      expect(mbc.isConnected()).to.equal(false);
      expect(mbc.connection).to.equal(null);
      expect(mbc.connect_timeout).to.not.equal(null);
    });

    it('should auto reconnect on connection error a limited number of times', function() {
      var mbc = mbcCreate(2);
      mock_logger.expect('log', 'Memcached ERROR: "ECONNREFUSED"');
      mock_logger.expect('log', 'Memcached removing connection, 0 outstanding');
      mock_logger.expect('log', 'Memcached reconnecting in 1s');
      mock_net.addEventOp('error', 'ECONNREFUSED', true);

      expect(mbc.isConnected()).to.equal(false);
      expect(mbc.connection).to.equal(null);
      expect(mbc.connect_timeout).to.not.equal(null);

      // Pretend timeout passed, try again
      mbc.connect();
      // fail again
      mock_logger.expect('log', 'Memcached ERROR: "ECONNREFUSED"');
      mock_logger.expect('log', 'Memcached removing connection, 0 outstanding');
      mock_logger.expect('log', 'Memcached reconnecting in 2s');
      mock_net.addEventOp('error', 'ECONNREFUSED', true);
      expect(mbc.connect_timeout).to.not.equal(null);

      // Pretend timeout passed, try again
      mbc.connect();
      // fail one last time, shouldn't reconnect
      mock_logger.expect('log', 'Memcached ERROR: "ECONNREFUSED"');
      mock_logger.expect('log', 'Memcached removing connection, 0 outstanding');
      mock_net.addEventOp('error', 'ECONNREFUSED', true);
      expect(mbc.connect_timeout).to.equal(null);
    });

    it('should auto reconnect a connection closed due to an event', function() {
      var mbc = mbcCreate(true);
      logExpectReconnect();
      logExpectEvent('Memcached END');
      mock_net.addEventOp('end', null, true);
      expect(mbc.isConnected()).to.equal(false);
      expect(mbc.connection).to.equal(null);
      expect(mbc.connect_timeout).to.not.equal(null);
    });

    it('should clear connect timeout on a connect call', function() {
      var mbc = mbcCreate(true);
      var mbc_connection = mbc.connection;
      logExpectReconnect();
      logExpectClose();
      mbc.close();
      mbc.connect();
      expect(mbc.isConnected()).to.equal(false);
      expect(mbc.connection).to.not.equal(null);
      expect(mbc.connection).to.not.equal(mbc_connection);
      expect(mbc.connect_timeout).to.equal(null);
      expect(mbc.connect_tries).to.equal(1);
    });

    it('should clear connect tries after connecting', function() {
      var mbc = mbcCreate(true);
      var mbc_connection = mbc.connection;
      logExpectReconnect();
      logExpectClose();
      mbc.close();
      logExpectConnect();
      mbc.connect();
      mock_net.fireConnect();
      expect(mbc.isConnected()).to.equal(true);
      expect(mbc.connection).to.not.equal(null);
      expect(mbc.connection).to.not.equal(mbc_connection);
      expect(mbc.connect_timeout).to.equal(null);
      expect(mbc.connect_tries).to.equal(0);
    });

    it('should limit auto reconnect delay', function() {
      var mbc = mbcCreate(true);
      mbc.connect_tries = 9;
      mock_logger.expect('log', 'Memcached reconnecting in 256s');
      logExpectClose();
      mbc.close();
      expect(mbc.isConnected()).to.equal(false);
      expect(mbc.connection).to.equal(null);
      expect(mbc.connect_timeout).to.not.equal(null);
    });

    it('should not auto reconnect a reconnecting connection', function() {
      var mbc = mbcCreate(true);
      logExpectReconnect();
      logExpectClose();
      mbc.close();
      mbc.reconnect();
      expect(mbc.isConnected()).to.equal(false);
      expect(mbc.connection).to.equal(null);
      expect(mbc.connect_timeout).to.not.equal(null);
    });

  });

  describe('#data event buffering', function() {
    it('should receive data', function (done) {
      var mbc = mbcCreate();
      mock_net.addReadOp(new Buffer([129]), true);
      process.nextTick(function () {
        expect(mbc.response_data_buffer).to.deep.equal(new Buffer([129]));
        done();
      });
    });

    it('should buffer multiple received data', function (done) {
      var mbc = mbcCreate();
      mock_net.addReadOp([new Buffer([129]), new Buffer([1])], true);
      process.nextTick(function () {
        expect(mbc.response_data_buffer).to.deep.equal(new Buffer([129, 1]));
        done();
      });
    });

    it('should buffer data without a full header', function (done) {
      var mbc = mbcCreate();
      // Send 23 bytes, full header is 24
      mock_net.addReadOp(new Buffer([129,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]), true);
      process.nextTick(function () {
        expect(mbc.response_data_buffer).to.deep.equal(new Buffer([129,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]));
        done();
      });
    });

    it('should buffer data without a full body', function (done) {
      var mbc = mbcCreate();
      // Send 24 bytes with body length of 1
      mock_net.addReadOp(new Buffer([129,1,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0]), true);
      process.nextTick(function () {
        expect(mbc.response_data_buffer).to.deep.equal(new Buffer([129,1,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0]));
        done();
      });
    });

    it('should continue buffering leftover data', function (done) {
      var mbc = mbcCreate();
      mock_net.addWriteOp(
        buf_set_abc_def,
        undefined,
        new Buffer([129,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,129])); // normal response plus an extra byte
      mbc.set('abc', 'def', undefined, function(err, res) {
        expect(err).to.equal(null);
        expect(res).to.equal('');
        process.nextTick(function() {
          expect(mbc.response_data_buffer).to.deep.equal(new Buffer([129]));
          done();
        });
      });
    });
  });

  describe('#request buffer pooling', function() {
    it('should pool request buffers', function (done) {
      var mbc = mbcCreate();
      mock_net.addWriteOp(
        buf_set_abc_def,
        undefined,
        buf_set_resp);
      mbc.set('abc', 'def', undefined, function(err, res) {
        expect(err).to.equal(null);
        expect(res).to.equal('');
        expect(mbc.request_buffer_pool.length).to.equal(1);
        done();
      });
    });

    it('should re-use a pooled request buffer', function (done) {
      var mbc = mbcCreate();
      mock_net.addWriteOp(
        buf_set_abc_def,
        undefined,
        buf_set_resp);
      mock_net.addWriteOp(
        buf_set_abc_def,
        undefined,
        buf_set_resp);
      mbc.set('abc', 'def', undefined, function(err, res) {
        expect(err).to.equal(null);
        expect(res).to.equal('');
        expect(mbc.request_buffer_pool.length).to.equal(1);
        mbc.set('abc', 'def', undefined, function(err, res) {
          expect(err).to.equal(null);
          expect(res).to.equal('');
          expect(mbc.request_buffer_pool.length).to.equal(1);
          done();
        });
      });
    });

    it('should make a new request buffer if nothing in the pool is big enough', function (done) {
      var mbc = mbcCreate();
      mock_net.addWriteOp(
        new Buffer([128,1,0,3,8,0,0,0,0,0,0,15,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,97,98,99,100,101,102,103]),
        undefined,
        buf_set_resp);
      mock_net.addWriteOp(
        buf_set_abc_def,
        undefined,
        buf_set_resp);
      mbc.set('abc', 'def', undefined, function(err, res) {
        expect(err).to.equal(null);
        expect(res).to.equal('');
        expect(mbc.request_buffer_pool.length).to.equal(1);
        mbc.set('abc', 'defg', undefined, function(err, res) {
          expect(err).to.equal(null);
          expect(res).to.equal('');
          expect(mbc.request_buffer_pool.length).to.equal(2);
          done();
        });
      });
    });

    it('should pick the smallest buffer', function (done) {
      var mbc = mbcCreate();
      mock_net.addWriteOp(
        new Buffer([128,1,0,3,8,0,0,0,0,0,0,15,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,97,98,99,100,101,102,103]),
        undefined,
        buf_set_resp);
      mock_net.addWriteOp(
        buf_set_abc_def,
        undefined,
        buf_set_resp);
      mbc.set('abc', 'def', undefined, function(err, res) {
        expect(err).to.equal(null);
        expect(res).to.equal('');
        expect(mbc.request_buffer_pool.length).to.equal(1);
        mbc.set('abc', 'defg', undefined, function(err, res) {
          expect(err).to.equal(null);
          expect(res).to.equal('');
          expect(mbc.request_buffer_pool.length).to.equal(2);
          var best = mbc.getRequestBuffer(38);
          expect(mbc.request_buffer_pool.length).to.equal(1);
          expect(best.length).to.equal(38);
          done();
        });
      });
    });
  });

  describe('#status handling', function() {
    it('should report status message', function (done) {
      var mbc = mbcCreate();
      mock_net.addWriteOp(
        buf_set_abc_def,
        undefined,
        new Buffer([129,1,0,0,0,0,0,2,0,0,0,3,0,0,0,0,0,0,0,0,0,0,0,0,97,98,99])); // status 2 'abc'
      mbc.set('abc', 'def', undefined, function(err, res) {
        expect(err).to.equal('2 abc');
        expect(res).to.equal(null);
        done();
      });
    });

    it('should cache status message', function (done) {
      var mbc = mbcCreate();
      mock_net.addWriteOp(
        buf_set_abc_def,
        undefined,
        new Buffer([129,1,0,0,0,0,0,2,0,0,0,3,0,0,0,0,0,0,0,0,0,0,0,0,97,97,97])); // status 2 'aaa'
      mock_net.addWriteOp(
        buf_set_abc_def,
        undefined,
        new Buffer([129,1,0,0,0,0,0,2,0,0,0,3,0,0,0,0,0,0,0,0,0,0,0,0,97,98,99])); // status 2 'abc'
      var first = false;
      mbc.set('abc', 'def', undefined, function(err, res) {
        expect(first).to.equal(false);
        first = true;
        expect(err).to.equal('2 abc');
        expect(res).to.equal(null);
      });
      mbc.set('abc', 'def', undefined, function(err, res) {
        expect(first).to.equal(true);
        expect(err).to.equal('2 abc'); // should be abs even though text was aaa because we cache it
        expect(res).to.equal(null);
        done();
      });
    });

    it('should handle blank status message', function (done) {
      var mbc = mbcCreate();
      mock_net.addWriteOp(
        buf_set_abc_def,
        undefined,
        new Buffer([129,1,0,0,0,0,0,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0])); // status 2 with no message
      mbc.set('abc', 'def', undefined, function(err, res) {
        expect(err).to.equal('2 Unknown error');
        expect(res).to.equal(null);
        done();
      });
    });

    it('should treate status 1 as non-error no result', function (done) {
      var mbc = mbcCreate();
      mock_net.addWriteOp(
        buf_set_abc_def,
        undefined,
        new Buffer([129,1,0,0,0,0,0,1,0,0,0,3,0,0,0,0,0,0,0,0,0,0,0,0,97,98,99])); // status 1 'abc'
      mbc.set('abc', 'def', undefined, function(err, res) {
        expect(err).to.equal(null);
        expect(res).to.equal(null);
        done();
      });
    });
  });

  describe('#error handling', function() {
    it('should report and close on null request', function (done) {
      var mbc = mbcCreate();
      logExpectErrorClose('Memcached null request, 0 outstanding');
      mock_net.addReadOp(new Buffer([129,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]), true);
      process.nextTick(function () {
        expect(mbc.isConnected()).to.equal(false);
        done();
      });
    });

    it('should report and close on mismatched opcode', function (done) {
      var mbc = mbcCreate();
      var buf_wrong_op = new Buffer([129,0,0,0,0,0,0,0,0,0,0,3,0,0,0,0,0,0,0,0,0,0,0,0,97,98,99]); // opcode 0
      logExpectErrorClose('Memcached request response opcode mismatch: request 1 response 0', 5);
      // Followup errors are for four resposes that are read after closing
      mock_logger.expect('error', 'Memcached null request, 0 outstanding');
      mock_logger.expect('error', 'Memcached null request, 0 outstanding');
      mock_logger.expect('error', 'Memcached null request, 0 outstanding');
      mock_logger.expect('error', 'Memcached null request, 0 outstanding');
      var count = 0;
      mock_net.addCallbackOp(function() {
        // At this point all the calls we made to memcached have made their callbacks
        // However, only one of the 5 requests we made has actually had its response read, the other four are still pending.
        // This means that right at this instant, none of the four 'null request' error logs we should see have shown up, and if we
        //  tried to mock_logger.checkExpectations() right now, it would assert.
        // However, if we call done() now, the test will pass _most_ of the time, because done() doesn't appear to immediately fire
        //  afterEach(), which leaves the pending responses a tiny sliver of time to get processed and generate the error logs.
        // But that's not always _enough_ time, so instead of calling done immediately, we'll verify the state is what we expect,
        //  and then wait for the pending responses before calling done().
        expect(count).to.equal(4);
        expect(mock_net.lastConnection().pending_responses).to.equal(4);
        expect(mock_logger.getExpecting().error).to.equal(4);
        function checkPending() {
          if (!mock_net.lastConnection().pending_responses) {
            return done();
          }
          console.log('mock_net still has ' + mock_net.lastConnection().pending_responses + ' pending responses, waiting...');
          setTimeout(checkPending, 10);
        }
        setTimeout(checkPending, 10);
      });
      mock_net.addWriteOp(buf_set_abc_def, undefined, buf_wrong_op);
      mock_net.addWriteOp(buf_set_abc_def, undefined, buf_wrong_op);
      mock_net.addWriteOp(buf_set_abc_def, undefined, buf_wrong_op);
      mock_net.addWriteOp(buf_set_abc_def, undefined, buf_wrong_op);
      mock_net.addWriteOp(buf_set_abc_def, undefined, buf_wrong_op);
      function check(err, res) {
        expect(count < 4).to.equal(true);
        count++;
        expect(err).to.equal('Memcached connection removed');
        expect(res).to.equal(null);
        expect(mbc.isConnected()).to.equal(false);
      }
      mbc.set('abc', 'def', undefined, check);
      mbc.set('abc', 'def', undefined, check);
      mbc.set('abc', 'def'); // Instance without a callback sandwiched between to make sure that case is handled
      mbc.set('abc', 'def', undefined, check);
      mbc.set('abc', 'def', undefined, check);
    });
  });

  describe('#event handling', function() {
    it('should report and close on end event', function () {
      var mbc = mbcCreate();
      logExpectEvent('Memcached END');
      mock_net.addEventOp('end', null, true);
      expect(mbc.isConnected()).to.equal(false);
    });

    it('should report and close on close event', function () {
      var mbc = mbcCreate();
      logExpectEvent('Memcached CLOSE');
      mock_net.addEventOp('close', null, true);
      expect(mbc.isConnected()).to.equal(false);
    });

    it('should report and close on timeout event', function () {
      var mbc = mbcCreate();
      logExpectEvent('Memcached TIMEOUT');
      mock_net.addEventOp('timeout', null, true);
      expect(mbc.isConnected()).to.equal(false);
    });

    it('should report and close on error event', function () {
      var mbc = mbcCreate();
      logExpectEvent('Memcached ERROR: "mock error"');
      mock_net.addEventOp('error', 'mock error', true);
      expect(mbc.isConnected()).to.equal(false);
    });
  });

  describe('#get', function() {
    it('should handle get', function (done) {
      var mbc = mbcCreate();
      mock_net.addWriteOp(
        buf_get_abc,
        undefined,
        new Buffer([129,0,0,0,4,0,0,0,0,0,0,7,0,0,0,0,0,0,0,0,0,0,0,1,0xde,0xad,0xbe,0xef,100,101,102]));
      mbc.get('abc', null, function(err, res) {
        expect(err).to.equal(null);
        expect(res).to.be.a('string');
        expect(res).to.equal('def');
        done();
      });
    });

    it('should handle binary get', function (done) {
      var mbc = mbcCreate(false, true);
      mock_net.addWriteOp(
        buf_get_abc,
        undefined,
        new Buffer([129,0,0,0,4,0,0,0,0,0,0,7,0,0,0,0,0,0,0,0,0,0,0,1,0xde,0xad,0xbe,0xef,100,101,102]));
      mbc.get('abc', null, function(err, res) {
        expect(err).to.equal(null);
        expect(Buffer.isBuffer(res)).to.equal(true);
        expect(res.toString()).to.equal('def');
        done();
      });
    });

    it('should handle get with cas', function (done) {
      var mbc = mbcCreate();
      mock_net.addWriteOp(
        buf_get_abc,
        undefined,
        new Buffer([129,0,0,0,4,0,0,0,0,0,0,7,0,0,0,0,0,0,0,0,0,0,0,1,0xde,0xad,0xbe,0xef,100,101,102]));
      mbc.get('abc', {cas: true}, function(err, res) {
        expect(err).to.equal(null);
        expect(res.val).to.equal('def');
        expect(res.cas.high).to.equal(0);
        expect(res.cas.low).to.equal(1);
        done();
      });
    });
  });

  describe('#set', function() {
    it('should handle set', function () {
      var mbc = mbcCreate();
      mock_net.addWriteOp(
        buf_set_abc_def,
        undefined,
        buf_set_resp);
      mbc.set('abc', 'def');
    });

    it('should handle binary set', function () {
      var mbc = mbcCreate();
      mock_net.addWriteOp(
        buf_set_abc_def,
        undefined,
        buf_set_resp);
      mbc.set('abc', new Buffer('def'));
    });

    it('should handle set with lifetime', function () {
      var mbc = mbcCreate();
      mock_net.addWriteOp(
        new Buffer([128,1,0,3,8,0,0,0,0,0,0,14,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0x0e,0x10,97,98,99,100,101,102]),
        undefined,
        buf_set_resp);
      mbc.set('abc', 'def', {lifetime: 60*60}); // 1 hour
    });

    it('should handle set with flags', function () {
      var mbc = mbcCreate();
      mock_net.addWriteOp(
        new Buffer([128,1,0,3,8,0,0,0,0,0,0,14,0,0,0,0,0,0,0,0,0,0,0,0,0xde,0xad,0xbe,0xef,0,0,0,0,97,98,99,100,101,102]),
        undefined,
        buf_set_resp);
      mbc.set('abc', 'def', {flags: 0xdeadbeef});
    });

    it('should handle set with cas', function () {
      var mbc = mbcCreate();
      mock_net.addWriteOp(
        new Buffer([128,1,0,3,8,0,0,0,0,0,0,14,0,0,0,0,0,0,0,1,0,0,0,2,0,0,0,0,0,0,0,0,97,98,99,100,101,102]),
        undefined,
        buf_set_resp);
      mbc.set('abc', 'def', {cas: {high: 1, low: 2}});
    });

    it('should handle set with callback', function (done) {
      var mbc = mbcCreate();
      mock_net.addWriteOp(
        buf_set_abc_def,
        undefined,
        buf_set_resp);
      mbc.set('abc', 'def', null, function(err, res) {
        expect(err).to.equal(null);
        expect(res).to.equal('');
        done();
      });
    });
  });

  describe('#add', function() {
    it('should handle add', function () {
      var mbc = mbcCreate();
      mock_net.addWriteOp(
        new Buffer([128,2,0,3,8,0,0,0,0,0,0,14,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,97,98,99,100,101,102]),
        undefined,
        new Buffer([129,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1]));
      mbc.add('abc', 'def');
    });
  });

  describe('#replace', function() {
    it('should handle replace', function () {
      var mbc = mbcCreate();
      mock_net.addWriteOp(
        new Buffer([128,3,0,3,8,0,0,0,0,0,0,14,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,97,98,99,100,101,102]),
        undefined,
        new Buffer([129,3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1]));
      mbc.replace('abc', 'def');
    });
  });

  describe('#del', function() {
    it('should handle del', function () {
      var mbc = mbcCreate();
      mock_net.addWriteOp(
        new Buffer([128,4,0,3,0,0,0,0,0,0,0,3,0,0,0,0,0,0,0,0,0,0,0,0,97,98,99]),
        undefined,
        new Buffer([129,4,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1]));
      mbc.del('abc');
    });
  });

  describe('#domains', function() {
    it('should call callback in the same domain as the request', function (done) {
      var mbc = mbcCreate();
      mock_net.addWriteOp(
        buf_get_abc,
        undefined,
        new Buffer([129,0,0,0,4,0,0,0,0,0,0,7,0,0,0,0,0,0,0,0,0,0,0,1,0xde,0xad,0xbe,0xef,100,101,102]));
      var domain = require('domain');
      var dom = domain.create();
      dom.run(function () {
        mbc.get('abc', null, function(err, res) {
          expect(process.domain).to.equal(dom);
          expect(err).to.equal(null);
          expect(res).to.be.a('string');
          expect(res).to.equal('def');
          done();
        });
      });
    });
  });

});
