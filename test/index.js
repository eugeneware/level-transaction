var expect = require('expect.js'),
    path = require('path'),
    rimraf = require('rimraf'),
    level = require('level'),
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
});
