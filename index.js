var after = require('after'),
    setImmediate = global.setImmediate || process.nextTick,
    unique = require('lodash.uniq'),
    intersection = require('lodash.intersection'),
    range = require('key-range'),
    futureStream = require('future-stream');

module.exports = tx;
function tx(db) {
  db.txPut = db.txPut || txPut.bind(db);
  db.txBatch = db.txBatch || txBatch.bind(db);
  db.txDel = db.txDel || txDel.bind(db);
  db.txGet = db.txGet || txGet.bind(db);
  db.txCreateReadStream = db.txCreateReadStream || txFutureStream(db, 'createReadStream');
  db.txCreateKeyStream = db.txCreateKeyStream || txFutureStream(db, 'createKeyStream');
  db.txCreateValueStream = db.txCreateValueStream || txFutureStream(db, 'createValueStream');
  db.txCreateWriteStream = db.txCreateWriteStream || txCreateWriteStream.bind(db);
  db._txKeys = [];
  db._txTimeout = 10000; // transaction timeout in ms
  return db;
}

function noop() {
}

function txFutureStream(db, methodName) {
  return function (options) {
    function check() {
      var keys = db._txKeys.filter(function (key) {
        return range(key, options);
      });
      return keys.length === 0;
    }
    if (check()) {
      return db[methodName](options);
    } else {
      return futureStream(db[methodName].bind(db, options), check);
    }
  };
}

function txCreateWriteStream(options) {
  var db = this;
  function check(data) {
    return !~db._txKeys.indexOf(data.key);
  }
  return futureStream.write(db.createWriteStream.bind(db, options), check);
}

function txGet(key, opts, cb) {
  if (typeof opts === 'function') {
    cb = opts;
    opts = undefined;
  }
  var db = this;
  (function check() {
    if (~db._txKeys.indexOf(key)) {
      setImmediate(check);
    } else {
      db.get(key, opts, cb);
    }
  })();
}

function txBatch(batch, opts, cb) {
  if (typeof opts === 'function') {
    cb = opts;
    opts = undefined;
  }

  var db = this;

  var rollbackBatch = [];

  var keys = unique(batch.map(function (op) {
    return op.key;
  }));

  (function doBatch() {
    if (intersection(keys, db._txKeys).length) return setImmediate(doBatch);

    db._txKeys = db._txKeys.concat(keys);

    var next = after(keys.length, _batch);
    keys.forEach(function (key) {
      db.get(key, function (err, value) {
        var type = 'put';
        if (err) {
          if (err.type !== 'NotFoundError') {
            return next(err);
          } else {
            type = 'del';
          }
        }
        rollbackBatch.push({ type: type, key: key, value: value });
        next();
      });
    });
  })();

  function unblockReads() {
    db._txKeys = [];
  }

  function _batch() {
    return db.batch.call(db, batch, opts, function (err) {
      if (err) return cb(err);

      var tid = setTimeout(rollback, (opts && opts.txTimeout) || db._txTimeout);

      function rollback(_cb) {
        clearTimeout(tid);
        unblockReads();
        return db.batch(rollbackBatch, function (err) {
          db.emit('rollback');
          (_cb || noop)(err);
        });
      }

      return cb(null, {
        commit: function (_cb) {
          clearTimeout(tid);
          unblockReads();
          setImmediate(function () {
            db.emit('commit');
            (_cb || noop)(null);
          });
        },
        rollback: rollback
      });
    });
  }
}

function txPut(key, value, opts, cb) {
  if (typeof opts === 'function') {
    cb = opts;
    opts = undefined;
  }

  var db = this;
  return db.txBatch([{ type: 'put', key: key, value: value }], opts, cb);
}

function txDel(key, opts, cb) {
  if (typeof opts === 'function') {
    cb = opts;
    opts = undefined;
  }

  var db = this;
  return db.txBatch([{ type: 'del', key: key }], opts, cb);
}
