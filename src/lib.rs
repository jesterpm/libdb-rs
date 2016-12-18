//! Rust bindings for Berkeley DB 4.8.
//!
//! # Examples
//! ```
//! # use std::str;
//! let env = libdb::EnvironmentBuilder::new()
//!     .flags(libdb::DB_CREATE | libdb::DB_RECOVER | libdb::DB_INIT_TXN | libdb::DB_INIT_MPOOL)
//!     .open()
//!     .unwrap();
//!
//! let txn = env.txn(None, libdb::DB_NONE).unwrap();
//!
//! let db = libdb::DatabaseBuilder::new()
//!     .environment(&env)
//!     .transaction(&txn)
//!     .flags(libdb::DB_CREATE)
//!     .open()
//!     .unwrap();
//!
//! txn.commit(libdb::CommitType::Inherit).expect("Commit failed!");
//!
//! let mut key   = String::from("key").into_bytes();
//! let mut value = String::from("value").into_bytes();
//! assert!(db.put(None, key.as_mut_slice(), value.as_mut_slice(), libdb::DB_NONE).is_ok());
//!
//! let ret = db.get(None, key.as_mut_slice(), libdb::DB_NONE);
//! assert!(ret.is_ok());
//! assert_eq!("value", str::from_utf8(ret.ok().unwrap().unwrap().as_slice()).unwrap());
//! ```

#[macro_use] extern crate bitflags;
extern crate libc;
extern crate libdb_sys;

pub mod db;
pub mod dbt;
pub mod error;
pub mod flags;

pub use db::CommitType;
pub use db::DbType;
pub use db::Database;
pub use db::DatabaseBuilder;
pub use db::Environment;
pub use db::EnvironmentBuilder;
pub use db::Transaction;
pub use error::Error;
pub use flags::*;






