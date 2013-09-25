module.exports = tx;
function tx(db) {
  db.txPut = db.txPut || txPut.bind(db);
  return db;
}

function txPut(key, value, opts, cb) {
  if (typeof opts === 'function') {
    cb = opts;
    opts = undefined;
  }

  var db = this;
  var args = [].slice.call(arguments);

  var rollbackBatch = [];
  db.get(key, function (err, value) {
    var type = 'put';
    if (err) {
      if (err.type !== 'NotFoundError') {
        return cb(err);
      } else {
        type = 'del';
      }
    }
    rollbackBatch.push({ type: type, key: key, value: value });
    put();
  });

  function put() {
    return db.put.call(db, key, value, opts, function (err) {
      if (err) return cb(err);
      return cb(null, {
        commit: function (_cb) {
          return _cb(null);
        },
        rollback: function (_cb) {
          db.batch(rollbackBatch, _cb);
        }
      });
    });
  }
}
