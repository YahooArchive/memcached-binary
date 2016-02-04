/*
 * Copyright 2016, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

var assert = require('assert');

var log_types = [
  'trace', 'info', 'log', 'warn', 'error'
];

function MockLogger() {
  this.reset();
}
MockLogger.prototype.reset = function () {
  this.last_log = {};
  this.log_count = 0;
  this.expecting = {};
  this.ignoring = {};
};
MockLogger.prototype.expect = function (type, match) {
  this.expecting[type] = this.expecting[type] || [];
  this.expecting[type].push(match || '');
};
MockLogger.prototype.ignore = function (type, match) {
  this.ignoring[type] = this.ignoring[type] || [];
  this.ignoring[type].push(match || '');
};
MockLogger.prototype.getLastLog = function (type) {
  var ret = this.last_log[type || 'log'];
  this.reset();
  return ret;
};
MockLogger.prototype.checkExpectations = function () {
  var mock_logger = this;
  log_types.forEach(function (log_type) {
    if (mock_logger.expecting[log_type] && mock_logger.expecting[log_type].length) {
      mock_logger.expecting[log_type].forEach(function (match) {
        console.log('Did not receive log expected to match "' + match + '"');
      });
      assert.ok(false, 'MockLogger: Expected a log message of type "' + log_type + '", but did not receive one');
    }
  });
  mock_logger.reset();
};
MockLogger.prototype.getExpecting = function () {
  var mock_logger = this;
  var expecting = {};
  log_types.forEach(function (log_type) {
    if (mock_logger.expecting[log_type] && mock_logger.expecting[log_type].length) {
      expecting[log_type] = mock_logger.expecting[log_type].length;
    }
  });
  return expecting;
};

log_types.forEach(function (log_type) {
  MockLogger.prototype[log_type] = function (msg) {
    msg = msg.toString();
    this.last_log[log_type] = msg;
    function check(arr) {
      if (!arr) {
        return false;
      }
      var ii;
      for (ii = 0; ii < arr.length; ++ii) {
        if (msg.indexOf(arr[ii]) >= 0) {
          arr.splice(ii, 1);
          return true;
        }
      }
      return false;
    }
    if (check(this.expecting[log_type])) {
      // expected, do not display at all
      return;
    }
    if (check(this.ignoring[log_type])) {
      // ignored
      return;
    }
    console[log_type](msg);
    assert.ok(false, 'Received unexpected log of type "' + log_type + '": "' + msg + '"');
  };
});

module.exports = MockLogger;
