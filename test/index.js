var expect = require('expect.js'),
    path = require('path'),
    rimraf = require('rimraf'),
    level = require('level'),
    range = require('range'),
    after = require('after'),
    setImmediate = global.setImmediate || process.nextTick,
    tx = require('..');

describe('level-transaction', function() {
  var db, dbPath = path.join(__dirname, '..', 'data', 'testdb');

  beforeEach(function(done) {
    rimraf.sync(dbPath);
    db = level(dbPath);
    done();
  });

  afterEach(function(done) {
    db.close(done);
  });

  it('should be able to run a transactional put', function(done) {
    db = tx(db);
    db.txPut('key 1', 'value 1', function (err, tx) {
      if (err) return done(err);
      tx.commit(get);
    });

    function get(err) {
      if (err) return done(err);
      db.get('key 1', function (err, value) {
        expect(value).to.equal('value 1');
        done();
      });
    }
  });

  it('should be able to run a transactional put and rollback', function(done) {
    db = tx(db);
    db.txPut('key 1', 'value 1', function (err, tx) {
      if (err) return done(err);
      tx.rollback(get);
    });

    function get(err) {
      if (err) return done(err);
      db.get('key 1', function (err, value) {
        expect(err.type).to.equal('NotFoundError');
        done();
      });
    }
  });

  it('should be able to run a transactional batch', function(done) {
    var batch = range(0, 10).map(function (i) {
      return {
        type: 'put',
        key: 'key ' + i,
        value: 'value ' + i
      };
    });

    db = tx(db);
    db.txBatch(batch, function (err, tx) {
      if (err) return done(err);
      tx.commit(get);
    });

    function get(err) {
      if (err) return done(err);
      db.get('key 7', function (err, value) {
        expect(value).to.equal('value 7');
        done();
      });
    }
  });

  it('should be able to run a transactional batch with rollback', function(done) {
    var batch = range(0, 10).map(function (i) {
      return {
        type: 'put',
        key: 'key ' + i,
        value: 'value ' + i
      };
    });

    db = tx(db);
    db.txBatch(batch, function (err, tx) {
      if (err) return done(err);
      tx.rollback(get);
    });

    var count = 0;
    function get(err) {
      if (err) return done(err);
      var keys = batch.map(function (op) {
        return op.key;
      });
      var next = after(keys.length, check);
      keys.forEach(function (key) {
        db.get(key, function (err, value) {
          expect(err.type).to.equal('NotFoundError');
          count++;
          next();
        });
      });
    }

    function check() {
      expect(count).to.equal(batch.length);
      done();
    }
  });

  it('should be able to run a transactional del', function(done) {
    db = tx(db);
    var batch = range(0, 10).map(function (i) {
      return {
        type: 'put',
        key: 'key ' + i,
        value: 'value ' + i
      };
    });

    db.batch(batch, del);

    function del(err) {
      if (err) return done(err);
      db.txDel('key 8', function (err, tx) {
        if (err) return done(err);
        tx.commit(get);
      });
    }

    function get(err) {
      if (err) return done(err);
      db.get('key 8', function (err, value) {
        expect(err.type).to.equal('NotFoundError');
        done();
      });
    }
  });

  it('should be able to run a transactional del with rollback', function(done) {
    db = tx(db);
    var batch = range(0, 10).map(function (i) {
      return {
        type: 'put',
        key: 'key ' + i,
        value: 'value ' + i
      };
    });

    db.batch(batch, del);

    function del(err) {
      if (err) return done(err);
      db.txDel('key 8', function (err, tx) {
        if (err) return done(err);
        tx.rollback(get);
      });
    }

    function get(err) {
      if (err) return done(err);
      db.get('key 8', function (err, value) {
        if (err) return done(err);
        expect(value).to.equal('value 8');
        done();
      });
    }
  });

  it('should block key reads during a transaction', function(done) {
    db = tx(db);
    var next = after(2, done);
    db.txPut('key 1', 'value 1', function (err, tx) {
      if (err) return done(err);
      get1();
      tx.commit(get2);
    });

    function get1() {
      db.txGet('key 1', function (err, value) {
        if (err) return done(err);
        expect(value).to.equal('value 1');
        next();
      });
    }

    function get2(err) {
      if (err) return done(err);
      db.txGet('key 1', function (err, value) {
        expect(value).to.equal('value 1');
        next();
      });
    }
  });

  it('should block key reads during a transaction with rollback', function(done) {
    db = tx(db);
    var next = after(2, done);
    db.txPut('key 1', 'value 1', function (err, tx) {
      if (err) return done(err);
      get1();
      tx.rollback(get2);
    });

    function get1() {
      db.txGet('key 1', function (err, value) {
        expect(err.type).to.equal('NotFoundError');
        next();
      });
    }

    function get2(err) {
      if (err) return done(err);
      db.txGet('key 1', function (err, value) {
        expect(err.type).to.equal('NotFoundError');
        next();
      });
    }
  });

  it('should block key reads (deletes) during a transaction', function(done) {
    db = tx(db);
    var batch = range(0, 10).map(function (i) {
      return {
        type: 'put',
        key: 'key ' + i,
        value: 'value ' + i
      };
    });

    db.batch(batch, del);

    var next = after(2, done);
    function del() {
      db.txDel('key 6', function (err, tx) {
        if (err) return done(err);
        get1();
        tx.commit(get2);
      });
    }

    function get1() {
      db.txGet('key 6', function (err, value) {
        expect(err.type).to.equal('NotFoundError');
        next();
      });
    }

    function get2(err) {
      if (err) return done(err);
      db.txGet('key 6', function (err, value) {
        expect(err.type).to.equal('NotFoundError');
        next();
      });
    }
  });

  it('should block key (deletes) during a transaction with rollback', function(done) {
    db = tx(db);
    var batch = range(0, 10).map(function (i) {
      return {
        type: 'put',
        key: 'key ' + i,
        value: 'value ' + i
      };
    });

    db.batch(batch, del);

    var next = after(2, done);
    function del() {
      db.txDel('key 6', function (err, tx) {
        if (err) return done(err);
        get1();
        tx.rollback(get2);
      });
    }

    function get1() {
      db.txGet('key 6', function (err, value) {
        if (err) return done(err);
        expect(value).to.equal('value 6');
        next();
      });
    }

    function get2(err) {
      if (err) return done(err);
      db.txGet('key 6', function (err, value) {
        if (err) return done(err);
        expect(value).to.equal('value 6');
        next();
      });
    }
  });

  it('should be able to set the transaction timeout', function(done) {
    db = tx(db);

    var start = Date.now();
    db.once('rollback', get1);
    db.txPut('key 1', 'value 1', { txTimeout: 200 }, function (err, tx) {
      if (err) return done(err);
    });

    function get1() {
      db.get('key 1', function (err, value) {
        // console.log(err, value);
        expect(err.type).to.equal('NotFoundError');
        done();
      });
    }
  });

  it('should block key writes during a transaction', function(done) {
    db = tx(db);
    var next = after(2, done);

    db.txPut('key 1', 'value 1', function (err, tx) {
      if (err) return done(err);
      put();
      tx.commit(get2);
    });

    function put() {
      db.txPut('key 1', 'value 2', function (err, tx) {
        if (err) return done(err);
        tx.commit(get1);
      });
    }

    function get1(err) {
      if (err) return done(err);
      db.txGet('key 1', function (err, value) {
        if (err) return done(err);
        expect(value).to.equal('value 2');
        next();
      });
    }

    function get2(err) {
      if (err) return done(err);
      setImmediate(function () {
        db.txGet('key 1', function (err, value) {
          if (err) return done(err);
          expect(value).to.equal('value 2');
          next();
        });
      });
    }
  });

  it('should be able to block readstreams on a transaction', function(done) {
    var batch = range(0, 10).map(function (i) {
      return {
        type: 'put',
        key: 'key ' + i,
        value: 'value ' + i
      };
    });

    db = tx(db);
    var start = Date.now();
    var delay = 250;
    db.txBatch(batch, function (err, tx) {
      if (err) return done(err);
      setTimeout(tx.commit.bind(tx), delay);
      stream(done);
    });

    function stream(cb) {
      var count = 0;
      db.txCreateReadStream({ start: 'key 5', end: 'key 7' })
        .on('data', function (data) {
          expect(Date.now()).to.be.above(start + delay);
          count++;
        })
        .on('end', function () {
          expect(count).to.equal(3);
          cb();
        });
    }
  });

  it('should be able to block keystreams on a transaction', function(done) {
    var batch = range(0, 10).map(function (i) {
      return {
        type: 'put',
        key: 'key ' + i,
        value: 'value ' + i
      };
    });

    db = tx(db);
    var start = Date.now();
    var delay = 250;
    db.txBatch(batch, function (err, tx) {
      if (err) return done(err);
      setTimeout(tx.commit.bind(tx), delay);
      stream(done);
    });

    function stream(cb) {
      var count = 0;
      db.txCreateKeyStream({ start: 'key 5', end: 'key 7' })
        .on('data', function (data) {
          expect(Date.now()).to.be.above(start + delay);
          expect(data).to.match(/^key [5-7]$/);
          count++;
        })
        .on('end', function () {
          expect(count).to.equal(3);
          cb();
        });
    }
  });

  it('should be able to block valuestreams on a transaction', function(done) {
    var batch = range(0, 10).map(function (i) {
      return {
        type: 'put',
        key: 'key ' + i,
        value: 'value ' + i
      };
    });

    db = tx(db);
    var start = Date.now();
    var delay = 250;
    db.txBatch(batch, function (err, tx) {
      if (err) return done(err);
      setTimeout(tx.commit.bind(tx), delay);
      stream(done);
    });

    function stream(cb) {
      var count = 0;
      db.txCreateValueStream({ start: 'key 5', end: 'key 7' })
        .on('data', function (data) {
          expect(Date.now()).to.be.above(start + delay);
          expect(data).to.match(/^value [5-7]$/);
          count++;
        })
        .on('end', function () {
          expect(count).to.equal(3);
          cb();
        });
    }
  });
});
