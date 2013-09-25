var after = require('after'),
    setImmediate = global.setImmediate || process.nextTick,
    unique = require('lodash.uniq');

module.exports = tx;
function tx(db) {
  db.txPut = db.txPut || txPut.bind(db);
  db.txBatch = db.txBatch || txBatch.bind(db);
  db.txDel = db.txDel || txDel.bind(db);
  return db;
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

  function _batch() {
    return db.batch.call(db, batch, opts, function (err) {
      if (err) return cb(err);
      return cb(null, {
        commit: function (_cb) {
          setImmediate(_cb.bind(null));
        },
        rollback: function (_cb) {
          return db.batch(rollbackBatch, _cb);
        }
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
