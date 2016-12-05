extern crate db;
extern crate tempdir;

use std::path::Path;
use tempdir::TempDir;

#[test]
fn open_close_open_test() {
    let dbdir     = TempDir::new("libdb-rs").expect("Expected temp dir");

    {
        let env       = open_test_env(dbdir.path());
        let db        = open_test_db(&env);    
        let mut key   = String::from("key").into_bytes();
        let mut value = String::from("value").into_bytes();
        assert!(db.put(None, key.as_mut_slice(), value.as_mut_slice(), db::DB_NONE).is_ok());
    }
    
    {
        let env     = open_test_env(dbdir.path());
        let db      = open_test_db(&env);   
        let mut key = String::from("key").into_bytes();
        assert_record_eq(&db, key.as_mut_slice(), "value");
    }
}

#[test]
fn test_transaction() {
    let dbdir     = TempDir::new("libdb-rs").expect("Expected temp dir");
    println!("Before creating the environment");
    let env       = open_test_env(dbdir.path());
    println!("After creating the environment");
    let db        = open_test_db(&env);    

    let mut key   = String::from("key").into_bytes();
    let mut value = String::from("value").into_bytes();

    // Test explicit abort
    {
        let txn = env.txn(None, db::DB_NONE).unwrap();
        let ret = db.put(Some(&txn), key.as_mut_slice(), value.as_mut_slice(), db::DB_NONE);
        print!("{:?}", ret);
        assert!(ret.is_ok());
        assert!(txn.abort().is_ok());
    }

    // Should have no record.
    assert_norecord(&db, key.as_mut_slice());

    // Test abort when out of scope
    {
        let txn = env.txn(None, db::DB_NONE).unwrap();
        db.put(Some(&txn), key.as_mut_slice(), value.as_mut_slice(), db::DB_NONE).expect("Failed to put");
    }

    // Should have no record.
    assert_norecord(&db, key.as_mut_slice());

    // Test commit
    {
        let txn = env.txn(None, db::DB_NONE).unwrap();
        db.put(Some(&txn), key.as_mut_slice(), value.as_mut_slice(), db::DB_NONE).expect("Failed to put");
        txn.commit(db::CommitType::Inherit).expect("Failed to commit");
    }

    // Should have no record.
    assert_record_eq(&db, key.as_mut_slice(), "value");
}

/// Helper to open a BDB environment for the test.
fn open_test_env(dir: &Path) -> db::Environment {
    db::EnvironmentBuilder::new()
        .home(dir)
        .flags(db::DB_CREATE | db::DB_RECOVER | db::DB_INIT_LOG | db::DB_INIT_TXN | db::DB_INIT_MPOOL)
        .open()
        .expect("Failed to open DB")
}

/// Helper to open a BDB DB for the test.
fn open_test_db(env: &db::Environment) -> db::Database {
    let txn = env.txn(None, db::DB_NONE).unwrap();
    let ret = db::DatabaseBuilder::new()
        .environment(env)
        .transaction(&txn)
        .file("db")
        .db_type(db::DbType::BTree)
        .flags(db::DB_CREATE)
        .open();
    match ret {
        Ok(db) => {
            txn.commit(db::CommitType::Inherit).expect("Commit failed");
            db
        },
        Err(e) => { panic!("Error: {:?}", e) }
    }
}

/// Helper to assert a record is missing in the database.
fn assert_norecord(db: &db::Database, key: &mut [u8]) {
    assert!(db.get(None, key, db::DB_NONE).unwrap().is_none());
}

/// Helper to assert a record has a specific value in the database.
fn assert_record_eq(db: &db::Database, key: &mut [u8], expected :&str) {
    match db.get(None, key, db::DB_NONE) {
        Ok(Some(value)) => assert_eq!(expected, String::from_utf8(value).unwrap()),
        _               => assert!(false)
    }
}