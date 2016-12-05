//! Rust bindings for Berkeley DB 4.8.
//!
//! # Examples
//! ```
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
//!     .db_type(libdb::DbType::BTree)
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
//! assert_eq!(value, ret.ok().unwrap().unwrap());
//! ```

#[macro_use] extern crate bitflags;
extern crate libc;

use std::marker;
use std::path::Path;
use std::ptr;

mod db_ffi;
mod error;
mod flags;

pub use db_ffi::DbType;
pub use error::Error;
pub use flags::*;

/// `EnvironmentBuilder` is used to configure and open a Berkeley DB environment.
pub struct EnvironmentBuilder {
    env_ptr: *mut db_ffi::DB_ENV,
    home: Option<std::ffi::CString>,
    flags: Flags,
    mode: i32,
}

impl EnvironmentBuilder {
    /// Create a new Berkeley DB environment builder.
    ///
    /// # Panics
    /// Panics if libdb fails to allocate the DB_ENV struct (e.g. malloc error).
    pub fn new() -> EnvironmentBuilder {
        unsafe {
            let mut env_ptr: *mut db_ffi::DB_ENV = ptr::null_mut();
            let ret = db_ffi::db_env_create(&mut env_ptr, 0);
            match ret {
                0 => EnvironmentBuilder {
                        env_ptr: env_ptr,
                        home: None,
                        flags: DB_NONE,
                        mode: 0,
                    },
                e => panic!("Could not instantiate DB_ENV: {}", e)
            }
        }
    }

    /// Specify the environment home directory.
    pub fn home<P: AsRef<Path>>(mut self, home: P) -> Self {
        self.home = Some(std::ffi::CString::new(home.as_ref().to_str().unwrap()).unwrap());
        self
    }

    /// Set the environment flags.
    pub fn flags(mut self, flags: Flags) -> Self {
        self.flags = flags;
        self
    }

    /// Set the file mode.
    pub fn mode(mut self, mode: i32) -> Self {
        self.mode = mode;
        self
    }

    /// Open the Berkeley DB Environment.
    pub fn open(mut self) -> Result<Environment, Error> {
        // Get a pointer to the home directory.
        let home_ptr = match self.home.as_ref() {
            Some(cstr) => cstr.as_ptr(),
            None => ptr::null()
        };

        unsafe {
            match ((*self.env_ptr).open.unwrap())(self.env_ptr, home_ptr, self.flags.bits(), self.mode) {
                0 => {
                    let env = Environment { env_ptr: self.env_ptr };
                    self.env_ptr = ptr::null_mut();
                    Ok(env)
                },
                e => Err(Error::new(e)),
            }
        }
    }
}

impl Drop for EnvironmentBuilder {
    fn drop(&mut self) {
        if ptr::null() != self.env_ptr {
            unsafe {
                ((*self.env_ptr).close.unwrap())(self.env_ptr, 0);
            }
        }
    }
}

/// The `Environment` object is the handle to a Berkeley DB environment.
///
/// # Examples
/// ```
/// let ret = libdb::EnvironmentBuilder::new()
///     .flags(libdb::DB_CREATE | libdb::DB_RECOVER | libdb::DB_INIT_LOG | libdb::DB_INIT_TXN)
///     .open();
/// assert!(ret.is_ok());
/// ```
pub struct Environment {
    env_ptr: *mut db_ffi::DB_ENV
}

impl Environment {
    /// Begin a new transaction in the environment.
    pub fn txn(&self, parent: Option<&Transaction>, flags: Flags) -> Result<Transaction, Error> {
        unsafe {
            let mut txn_ptr: *mut db_ffi::DB_TXN = ptr::null_mut();
            let ret = ((*self.env_ptr).txn_begin.unwrap())(self.env_ptr, unwrap_txn_ptr(parent), &mut txn_ptr, flags.bits());
            match ret {
                0 => Ok(Transaction { txn_ptr: txn_ptr }),
                e => Err(Error::new(e)),
            }
        }
    }
}

impl Drop for Environment {
    fn drop(&mut self) {
        if ptr::null() != self.env_ptr {
            unsafe {
                ((*self.env_ptr).close.unwrap())(self.env_ptr, 0);
            }
        }
    }
}

/// The `Transaction` object is the handle for a transaction.
pub struct Transaction {
    txn_ptr: *mut db_ffi::DB_TXN,
}

#[repr(u32)]
pub enum CommitType {
    /// Inherit the commit mode from the transaction or the environment.
    Inherit = 0,
    /// Do not synchronously flush the log.
    NoSync = db_ffi::DB_TXN_NOSYNC,
    /// Synchronously flush the log.
    Sync = db_ffi::DB_TXN_SYNC,
}

impl Transaction {
    /// Complete the transaction normally.
    pub fn commit(mut self, mode: CommitType) -> Result<(), Error> {
        unsafe {
            let ret = match ((*self.txn_ptr).commit.unwrap())(self.txn_ptr, mode as u32) {
                0 => Ok(()),
                e => Err(Error::new(e))
            };
            self.txn_ptr = ptr::null_mut();
            ret
        }
    }

    /// Termination of the transaction.
    /// 
    /// The log is played backward, and any necessary undo operations are done.
    pub fn abort(mut self) -> Result<(), Error> {
        unsafe {
            let ret = match ((*self.txn_ptr).abort.unwrap())(self.txn_ptr) {
                0 => Ok(()),
                e => Err(Error::new(e))
            };
            self.txn_ptr = ptr::null_mut();
            ret
        }
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if ptr::null() != self.txn_ptr {
            unsafe {
                // Nothing needs to be done if this fails...
                ((*self.txn_ptr).abort.unwrap())(self.txn_ptr);
            }
        }
    }
}

/// Helper which returns a *DB_TXN or nullptr as appropriate.
fn unwrap_txn_ptr(txn: Option<&Transaction>) -> *mut db_ffi::DB_TXN {
    match txn {
        Some(txn) => txn.txn_ptr,
        None      => ptr::null_mut()
    }
}

/// `DatabaseBuilder` is used to configure and open a database.
pub struct DatabaseBuilder<'a, 'b> {
    // DatabaseBuilder must not outlive its environment.
    //_env: std::marker::PhantomData<&'a Environment>,
    env: Option<&'a Environment>,
    txn: Option<&'b Transaction>,
    file: Option<std::ffi::CString>,
    name: Option<std::ffi::CString>,
    flags: Flags,
    mode: i32,
    db_type: DbType,
}

impl<'a, 'b> DatabaseBuilder<'a, 'b> {
    /// Create a new DatabaseBuilder.
    pub fn new() -> DatabaseBuilder<'a, 'b> {
        DatabaseBuilder {
            env: None,
            txn: None,
            file: None,
            name: None,
            flags: DB_NONE,
            mode: 0,
            db_type: DbType::Unknown,
        }
    }

    /// Open the database within an environment.
    pub fn environment(mut self, env: &'a Environment) -> Self {
        self.env = Some(env);
        self
    }

    /// Open the database within a transaction.
    pub fn transaction(mut self, txn: &'b Transaction) -> Self {
        self.txn = Some(txn);
        self
    }

    /// Specify the database file.
    pub fn file<P: AsRef<Path>>(mut self, file: P) -> Self {
        self.file = Some(std::ffi::CString::new(file.as_ref().to_str().unwrap()).unwrap());
        self
    }

    /// Specify the database name.
    pub fn name(mut self, name: &str) -> Self {
        self.name = Some(std::ffi::CString::new(name).unwrap());
        self
    }

    /// Set the database flags.
    pub fn flags(mut self, flags: Flags) -> Self {
        self.flags = flags;
        self
    }

    /// Set the file mode.
    pub fn mode(mut self, mode: i32) -> Self {
        self.mode = mode;
        self
    }

    /// Set the database type.
    pub fn db_type(mut self, db_type: DbType) -> Self {
        self.db_type = db_type;
        self
    }

    /// Open the database represented by the file and database.
    ///
    /// # Panics
    /// Panics if libdb fails to create the DB struct (e.g. malloc error).
    pub fn open(self) -> Result<Database<'a>, Error> {
        // Get the DB_ENV pointer
        let env_ptr = match self.env {
            Some(env) => env.env_ptr,
            None      => ptr::null_mut()
        };
        
        // Get the file name pointer.
        let file_ptr = match self.file.as_ref() {
            Some(cstr) => cstr.as_ptr(),
            None => ptr::null()
        };

        // Get the database name pointer.
        let database_ptr = match self.name.as_ref() {
            Some(cstr) => cstr.as_ptr(),
            None => ptr::null()
        };

        unsafe {
            // Create the DB struct
            let mut db: *mut db_ffi::DB = ptr::null_mut();
            let ret = db_ffi::db_create(&mut db, env_ptr, 0);
            if ret != 0 {
                panic!("Could not instantiate DB. errno = {}", ret);
            }

            // Open the database
            let ret = ((*db).open.unwrap())(db, unwrap_txn_ptr(self.txn), file_ptr, database_ptr, self.db_type, self.flags.bits(), self.mode);
            match ret {
                0 => Ok(Database { db: db, _env: std::marker::PhantomData }),
                e => {
                    ((*db).close.unwrap())(db, 0);
                    Err(Error::new(e))
                },
            }
        }
    }
}

/// `Database` is the handle for a single Berkeley DB database.
///
/// # Examples
/// ```
/// let ret = libdb::DatabaseBuilder::new()
///     .db_type(libdb::DbType::BTree)
///     .flags(libdb::DB_CREATE)
///     .open();
/// assert!(ret.is_ok())
/// ```
pub struct Database<'a> {
    // Database must not outlive its Environment.
    _env: marker::PhantomData<&'a Environment>,
    db: *mut db_ffi::DB
}

impl<'a> Database<'a> {
    /// Get a key/data pair from the database.
    ///
    /// # Examples
    ///
    /// # Record Found
    /// ```
    /// # let db = libdb::DatabaseBuilder::new()
    /// #    .db_type(libdb::DbType::BTree)
    /// #    .flags(libdb::DB_CREATE)
    /// #    .open()
    /// #    .unwrap();
    /// // Note: BDB requires that the key be mutable.
    /// let mut key   = String::from("key").into_bytes();
    /// let mut value = String::from("value").into_bytes();
    /// assert!(db.put(None, key.as_mut_slice(), value.as_mut_slice(), libdb::DB_NONE).is_ok());
    ///
    /// let ret = db.get(None, key.as_mut_slice(), libdb::DB_NONE);
    /// assert!(ret.is_ok());
    /// assert_eq!(value, ret.ok().unwrap().unwrap());
    /// ```
    ///
    /// ## Record Not Found
    /// ```
    /// # let db = libdb::DatabaseBuilder::new()
    /// #    .db_type(libdb::DbType::BTree)
    /// #    .flags(libdb::DB_CREATE)
    /// #    .open()
    /// #    .unwrap();
    /// // Note: BDB requires that the key be mutable.
    /// let mut key = String::from("key2").into_bytes();
    /// let ret = db.get(None, key.as_mut_slice(), libdb::DB_NONE);
    /// println!("{:?}", ret);
    /// assert!(ret.is_ok());
    /// assert!(ret.unwrap().is_none());
    /// ```
    pub fn get(&self, txn: Option<&Transaction>, key: &mut [u8], flags: Flags) -> Result<Option<Vec<u8>>, Error> {
        let mut key_dbt: db_ffi::DBT = Default::default();
        key_dbt.data = key.as_mut_ptr();
        key_dbt.size = key.len() as u32;

        let mut data_dbt: db_ffi::DBT = Default::default();
        data_dbt.flags = db_ffi::DB_DBT_MALLOC;
        
        unsafe {
            match ((*self.db).get.unwrap())(self.db, unwrap_txn_ptr(txn), &mut key_dbt, &mut data_dbt, flags.bits()) {
                0 => {
                    let len = data_dbt.size as usize;
                    let mut data = Vec::with_capacity(len);
                    data.set_len(len);
                    ptr::copy(data_dbt.data, data.as_mut_ptr(), len);
                    libc::free(data_dbt.data as *mut libc::c_void);
                    Ok(Some(data))
                },
                error::DB_NOTFOUND => Ok(None),
                e => Err(Error::new(e))
            }
        }
    }

    /// Store a key/data pair in the database.
    ///
    /// # Examples
    /// ```
    /// # let db = libdb::DatabaseBuilder::new()
    /// #    .db_type(libdb::DbType::BTree)
    /// #    .flags(libdb::DB_CREATE)
    /// #    .open()
    /// #    .unwrap();
    /// // Note: BDB requires that the key and value be mutable.
    /// let mut key   = String::from("key").into_bytes();
    /// let mut value = String::from("value").into_bytes();
    /// let ret = db.put(None, key.as_mut_slice(), value.as_mut_slice(), libdb::DB_NONE);
    /// assert!(ret.is_ok());
    /// ```
    pub fn put(&self, txn: Option<&Transaction>, key: &mut [u8], data: &mut [u8], flags: Flags) -> Result<(), Error> {
        let mut key_dbt: db_ffi::DBT = Default::default();
        key_dbt.data = key.as_mut_ptr();
        key_dbt.size = key.len() as u32;

        let mut data_dbt: db_ffi::DBT = Default::default();
        data_dbt.data = data.as_mut_ptr();
        data_dbt.size = data.len() as u32;

        unsafe {
            match ((*self.db).put.unwrap())(self.db, unwrap_txn_ptr(txn), &mut key_dbt, &mut data_dbt, flags.bits()) {
                0 => Ok(()),
                e => Err(Error::new(e))
            }
        }
    }
}

impl<'a> Drop for Database<'a> {
    fn drop(&mut self) {
        unsafe {
            ((*self.db).close.unwrap())(self.db, 0);
        }
    }
}