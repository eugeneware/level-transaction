# level-transaction

Transactions, commits and rollbacks for leveldb/levelup databases.

[![build status](https://secure.travis-ci.org/eugeneware/level-transaction.png)](http://travis-ci.org/eugeneware/level-transaction)

**NB: This module is still under active development and is not to be used in production**

## Installation

This module is installed via npm:

``` bash
$ npm install level-transaction
```

## Example Usage

``` js
var level = require('level');
var levelTransaction = require('level-transaction');
var db = level('/my/db/path');

// add transaction methods
db = levelTransaction(db);

// Commit a put transaction
db.txPut('key 1', 'value 1', function (err, tx) {
  if (err) throw err;
  tx.commit(function (err) {
    if (err) throw err;
    // transaction is now written
    // key 1 => value 1
  });
});

// Rollback a put transaction
db.txPut('key 1', 'value 1', function (err, tx) {
  if (err) throw err;
  tx.rollback(function (err) {
    if (err) throw err;
    // transaction is rolled back
    // key 1 doesn't exist
  });
});

// Rollback a batch transaction
db.txBatch([{ type: 'put', key: 'k1', value: 'v2' },
            { type: 'del', key: 'k2'} ],
  function (err, tx) {
    if (err) throw err;
    tx.rollback(function (err) {
      if (err) throw err;
      // transaction has been rolled back
      // k1 doesn't exist, k2 (if it existed before), still exists
    });
  });
```

## db API

### db#txPut(key, value[, opts][, callback])

Put the key, value pair to the database, but wrapped in a transaction.

The callback (unlike with the standard levelup API) will also return a `tx`
parameter which has a `commit` and `rollback` method on it.

Any `txPut` operations will block any other `txPut`, `txGet`, `txDel`, and
`txBatch` operations where the keys intersect with the key being written for
this `txPut` operation.

### db#txGet(key, value[, opts][, callback])

Gets the `key` from the database.

However, unlike a normal levelup `get`, a `txGet` will block and wait for
any preceding `txPut`, `txDel`, `txBatch` operations that also contain the
`key` that is being fetched by `txGet`.

NB: normal `#get` operations do not have this blocking behaviour. If you wish
to have your gets wait for writes, then use `#txGet` and not `#get`.

### db#txDel(key[, opts][, callback])

Delete the key `key` from database, but wrapped in a transaction.

The callback (unlike with the standard levelup API) will also return a `tx`
parameter which has a `commit` and `rollback` method on it.

Any `txDel` operations will block any other `txPut`, `txGet`, `txDel`, and
`txBatch` operations where the keys intersect with the key being written for
this `txPut` operation.

### db#txBatch(opArray,[, opts][, callback])

Executes the array `opArray` of levelup operations wrapped in a single
transaction.

The callback (unlike with the standard levelup API) will also return a `tx`
parameter which has a `commit` and `rollback` method on it.

Any `txBatch` operations will block any other `txPut`, `txGet`, `txDel`, and
`txBatch` operations where the keys intersect with the key being written for
this `txPut` operation.

## transaction object API

The transaction object gets returned as a second parameter in the callbacks of
`db#txPut`, `db#txDel`, and `db#txBatch`.

### tx#commit([callback])

The callback is a standard node.js callback which takes an `err` object as it's
first parameter. When you are executing in the body of the commit function
you are guaranteed to have your transaction committed to the database.

### tx#rollback([callback])

The callback is a standard node.js callback which takes an `err` object as it's
first parameter. When you are executing in the body of the commit function
you are guaranteed to have your transaction rolled back to the state of the
database prior to the transaction.
