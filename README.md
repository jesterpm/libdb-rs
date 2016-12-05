libdb-rs
=======

Rust bindings for Berkeley DB 4.8.

[![Build Status](https://travis-ci.org/jesterpm/libdb-rs.svg?branch=master)](https://travis-ci.org/jesterpm/libdb-rs)

## Example

```rust
let env = db::EnvironmentBuilder::new()
    .flags(db::DB_CREATE | db::DB_RECOVER | db::DB_INIT_TXN | db::DB_INIT_MPOOL)
    .open()
    .unwrap();

let txn = env.txn(None, db::DB_NONE).unwrap();

let db = db::DatabaseBuilder::new()
    .environment(&env)
    .transaction(&txn)
    .db_type(db::DbType::BTree)
    .flags(db::DB_CREATE)
    .open()
    .unwrap();

txn.commit(db::CommitType::Inherit).expect("Commit failed!");

let mut key   = String::from("key").into_bytes();
let mut value = String::from("value").into_bytes();
db.put(None, key.as_mut_slice(), value.as_mut_slice(), db::DB_NONE).expect("Put failed!");

let result = db.get(None, key.as_mut_slice(), db::DB_NONE).unwrap();
println!("{:?}", result);
```

