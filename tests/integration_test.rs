extern crate libdb;
extern crate tempdir;

use std::str;
use std::path::Path;
use tempdir::TempDir;
use libdb::Flags;

#[test]
fn open_close_open_test() {
    let dbdir     = TempDir::new("libdb-rs").expect("Expected temp dir");

    {
        let (env, db) = open_test_db(dbdir.path());
        let mut key   = String::from("key").into_bytes();
        let mut value = String::from("value").into_bytes();
        assert!(db.put(None, key.as_mut_slice(), value.as_mut_slice(), Flags::DB_NONE).is_ok());
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
        let txn = env.txn(None, Flags::DB_NONE).unwrap();
        let ret = db.put(Some(&txn), key.as_mut_slice(), value.as_mut_slice(), Flags::DB_NONE);
        print!("{:?}", ret);
        assert!(ret.is_ok());
        assert!(txn.abort().is_ok());
    }

    // Should have no record.
    assert_norecord(&db, key.as_mut_slice());

    // Test abort when out of scope
    {
        let txn = env.txn(None, Flags::DB_NONE).unwrap();
        db.put(Some(&txn), key.as_mut_slice(), value.as_mut_slice(), Flags::DB_NONE).expect("Failed to put");
    }

    // Should have no record.
    assert_norecord(&db, key.as_mut_slice());

    // Test commit
    {
        let txn = env.txn(None, Flags::DB_NONE).unwrap();
        db.put(Some(&txn), key.as_mut_slice(), value.as_mut_slice(), Flags::DB_NONE).expect("Failed to put");
        txn.commit(libdb::CommitType::Inherit).expect("Failed to commit");
    }

    // Should have record
    assert_record_eq(&db, key.as_mut_slice(), "value");
}

#[test]
fn test_cursor() {
    let dbdir     = TempDir::new("libdb-rs").expect("Expected temp dir");
    let (env, db) = open_test_db(dbdir.path());

    let mut key_a   = String::from("testkeyA").into_bytes();
    let mut value_a = String::from("testvalueA").into_bytes();
    let mut key_b   = String::from("testkeyB").into_bytes();
    let mut value_b = String::from("testvalueB").into_bytes();

    // commit test values
    {
        let txn = env.txn(None, Flags::DB_NONE).unwrap();
        db.put(Some(&txn), key_a.as_mut_slice(), value_a.as_mut_slice(), Flags::DB_NONE).expect("Failed to put");
        txn.commit(libdb::CommitType::Inherit).expect("Failed to commit");
    }
    {
        let txn = env.txn(None, Flags::DB_NONE).unwrap();
        db.put(Some(&txn), key_b.as_mut_slice(), value_b.as_mut_slice(), Flags::DB_NONE).expect("Failed to put");
        txn.commit(libdb::CommitType::Inherit).expect("Failed to commit");
    }

    // get cursor and iterate
    let mut cursor = db.cursor().expect("Failed to get cursor");
    {
        let (key_dbt, data_dbt) = cursor.next().expect("Could not walk cursor");
        assert_eq!("testkeyA", str::from_utf8(key_dbt.unwrap().as_slice()).unwrap());
        assert_eq!("testvalueA", str::from_utf8(data_dbt.unwrap().as_slice()).unwrap());
    }
    {
        let (key_dbt, data_dbt) = cursor.next().expect("Could not walk cursor");
        assert_eq!("testkeyB", str::from_utf8(key_dbt.unwrap().as_slice()).unwrap());
        assert_eq!("testvalueB", str::from_utf8(data_dbt.unwrap().as_slice()).unwrap());
    }
}

/// Helper to open a BDB environment for the test.
fn open_test_db(dir: &Path) -> (libdb::Environment, libdb::Database) {
    let env = libdb::EnvironmentBuilder::new()
        .home(dir)
        .flags(Flags::DB_CREATE | Flags::DB_RECOVER | Flags::DB_INIT_LOG | Flags::DB_INIT_TXN | Flags::DB_INIT_MPOOL)
        .open()
        .expect("Failed to open DB");

    let txn = env.txn(None, Flags::DB_NONE).unwrap();
    let ret = libdb::DatabaseBuilder::new()
        .transaction(&txn)
        .environment(&env)
        .file("db")
        .flags(Flags::DB_CREATE)
        .open();

    match ret.as_ref() {
        Ok(db) => txn.commit(libdb::CommitType::Inherit).expect("Commit failed"),
        Err(e) => { panic!("Error: {:?}", e) }
    }

    (env, ret.unwrap())
}

/// Helper to assert a record is missing in the database.
fn assert_norecord(db: &libdb::Database, key: &mut [u8]) {
    assert!(db.get(None, key, Flags::DB_NONE).unwrap().is_none());
}

/// Helper to assert a record has a specific value in the database.
fn assert_record_eq(db: &libdb::Database, key: &mut [u8], expected :&str) {
    match db.get(None, key, Flags::DB_NONE) {
        Ok(Some(value)) => assert_eq!(expected, str::from_utf8(value.as_slice()).unwrap()),
        _               => assert!(false)
    }
}
