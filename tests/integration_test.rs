extern crate libdb;
extern crate tempdir;

use std::str;
use std::path::Path;
use tempdir::TempDir;

#[test]
fn open_close_open_test() {
    let dbdir     = TempDir::new("libdb-rs").expect("Expected temp dir");

    {
        let (env, db) = open_test_db(dbdir.path());
        let mut key   = String::from("key").into_bytes();
        let mut value = String::from("value").into_bytes();
        assert!(db.put(None, key.as_mut_slice(), value.as_mut_slice(), libdb::DB_NONE).is_ok());
    }
    
    {
        let (env, db) = open_test_db(dbdir.path());
        let mut key = String::from("key").into_bytes();
        assert_record_eq(&db, key.as_mut_slice(), "value");
    }
}

#[test]
fn test_transaction() {
    let dbdir     = TempDir::new("libdb-rs").expect("Expected temp dir");
    let (env, db) = open_test_db(dbdir.path());

    let mut key   = String::from("key").into_bytes();
    let mut value = String::from("value").into_bytes();

    // Test explicit abort
    {
        let txn = env.txn(None, libdb::DB_NONE).unwrap();
        let ret = db.put(Some(&txn), key.as_mut_slice(), value.as_mut_slice(), libdb::DB_NONE);
        print!("{:?}", ret);
        assert!(ret.is_ok());
        assert!(txn.abort().is_ok());
    }

    // Should have no record.
    assert_norecord(&db, key.as_mut_slice());

    // Test abort when out of scope
    {
        let txn = env.txn(None, libdb::DB_NONE).unwrap();
        db.put(Some(&txn), key.as_mut_slice(), value.as_mut_slice(), libdb::DB_NONE).expect("Failed to put");
    }

    // Should have no record.
    assert_norecord(&db, key.as_mut_slice());

    // Test commit
    {
        let txn = env.txn(None, libdb::DB_NONE).unwrap();
        db.put(Some(&txn), key.as_mut_slice(), value.as_mut_slice(), libdb::DB_NONE).expect("Failed to put");
        txn.commit(libdb::CommitType::Inherit).expect("Failed to commit");
    }

    // Should have no record.
    assert_record_eq(&db, key.as_mut_slice(), "value");
}

/// Helper to open a BDB environment for the test.
fn open_test_db(dir: &Path) -> (libdb::Environment, libdb::Database) {
    let env = libdb::EnvironmentBuilder::new()
        .home(dir)
        .flags(libdb::DB_CREATE | libdb::DB_RECOVER | libdb::DB_INIT_LOG | libdb::DB_INIT_TXN | libdb::DB_INIT_MPOOL)
        .open()
        .expect("Failed to open DB");

    let txn = env.txn(None, libdb::DB_NONE).unwrap();
    let ret = libdb::DatabaseBuilder::new()
        .transaction(&txn)
        .environment(&env)
        .file("db")
        .flags(libdb::DB_CREATE)
        .open();

    match ret.as_ref() {
        Ok(db) => txn.commit(libdb::CommitType::Inherit).expect("Commit failed"),
        Err(e) => { panic!("Error: {:?}", e) }
    }

    (env, ret.unwrap())
}

/// Helper to assert a record is missing in the database.
fn assert_norecord(db: &libdb::Database, key: &mut [u8]) {
    assert!(db.get(None, key, libdb::DB_NONE).unwrap().is_none());
}

/// Helper to assert a record has a specific value in the database.
fn assert_record_eq(db: &libdb::Database, key: &mut [u8], expected :&str) {
    match db.get(None, key, libdb::DB_NONE) {
        Ok(Some(value)) => assert_eq!(expected, str::from_utf8(value.as_slice()).unwrap()),
        _               => assert!(false)
    }
}