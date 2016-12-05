libdb-rs
=======

Rust bindings for Berkeley DB 4.8.

[![Build Status](https://travis-ci.org/jesterpm/libdb-rs.svg?branch=master)](https://travis-ci.org/jesterpm/libdb-rs)

## Example

```rust
extern crate libdb;

let env = libdb::EnvironmentBuilder::new()
    .flags(libdb::DB_CREATE | libdb::DB_RECOVER | libdb::DB_INIT_TXN | libdb::DB_INIT_MPOOL)
    .open()
    .unwrap();

let txn = env.txn(None, libdb::DB_NONE).unwrap();

let db = libdb::DatabaseBuilder::new()
    .environment(&env)
    .transaction(&txn)
    .db_type(libdb::DbType::BTree)
    .flags(libdb::DB_CREATE)
    .open()
    .unwrap();

txn.commit(libdb::CommitType::Inherit).expect("Commit failed!");

let mut key   = String::from("key").into_bytes();
let mut value = String::from("value").into_bytes();
db.put(None, key.as_mut_slice(), value.as_mut_slice(), libdb::DB_NONE).expect("Put failed!");

let result = db.get(None, key.as_mut_slice(), libdb::DB_NONE).unwrap();
println!("{:?}", result);
```

