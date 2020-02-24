use std::ffi::CString;
use std::path::Path;
use std::ptr;
use std::sync::Arc;

use libdb_sys::ffi as db_ffi;

use super::dbt::DBT;
use super::error;
use super::error::Error;

#[cfg(all(not(feature = "v5_3"), not(feature = "v4_8")))] use super::flags_5_3::Flags;
#[cfg(feature = "v5_3")] use super::flags_5_3::Flags;
#[cfg(feature = "v4_8")] use super::flags_4_8::Flags;

pub type Environment = Arc<Env>;
pub type Database = Arc<Db>;

/// `EnvironmentBuilder` is used to configure and open a Berkeley DB environment.
pub struct EnvironmentBuilder {
    env_ptr: *mut db_ffi::DB_ENV,
    home: Option<CString>,
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
                        flags: Flags::DB_NONE,
                        mode: 0,
                    },
                e => panic!("Could not instantiate DB_ENV: {}", e)
            }
        }
    }

    /// Specify the environment home directory.
    pub fn home<P: AsRef<Path>>(mut self, home: P) -> Self {
        self.home = Some(CString::new(home.as_ref().to_str().unwrap()).unwrap());
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
                    let env = Env {
                        env_ptr: self.env_ptr,
                    };
                    self.env_ptr = ptr::null_mut();
                    Ok(Arc::new(env))
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
/// use libdb::Flags;
///
/// let ret = libdb::EnvironmentBuilder::new()
///     .flags(Flags::DB_CREATE | Flags::DB_RECOVER | Flags::DB_INIT_LOG | Flags::DB_INIT_TXN)
///     .open();
/// assert!(ret.is_ok());
/// ```
pub struct Env {
    env_ptr: *mut db_ffi::DB_ENV,
}

impl Env {
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

impl Drop for Env {
    fn drop(&mut self) {
        if ptr::null() != self.env_ptr {
            unsafe {
                ((*self.env_ptr).close.unwrap())(self.env_ptr, 0);
            }
        }
    }
}

pub enum DbType {
    BTree,
    Hash,
    Recno,
    Queue,
    Any,
}

impl From<DbType> for db_ffi::DBTYPE {
    fn from(flavor: DbType) -> Self {
        match flavor {
            DbType::BTree => db_ffi::DBTYPE_DB_BTREE,
            DbType::Hash => db_ffi::DBTYPE_DB_HASH,
            DbType::Recno => db_ffi::DBTYPE_DB_RECNO,
            DbType::Queue => db_ffi::DBTYPE_DB_QUEUE,
            DbType::Any => db_ffi::DBTYPE_DB_UNKNOWN,
        }
    }
}

/// `DatabaseBuilder` is used to configure and open a database.
pub struct DatabaseBuilder<'a> {
    // DatabaseBuilder must not outlive its environment.
    //_env: std::marker::PhantomData<&'a Environment>,
    env: Option<Environment>,
    txn: Option<&'a Transaction>,
    file: Option<CString>,
    name: Option<CString>,
    flags: Flags,
    mode: i32,
    db_type: DbType,
}

impl<'a> DatabaseBuilder<'a> {
    /// Create a new DatabaseBuilder.
    pub fn new() -> DatabaseBuilder<'a> {
        DatabaseBuilder {
            env: None,
            txn: None,
            file: None,
            name: None,
            flags: Flags::DB_NONE,
            mode: 0,
            db_type: DbType::BTree,
        }
    }

    /// Open the database within an environment.
    pub fn environment(mut self, env: &Environment) -> Self {
        self.env = Some(env.clone());
        self
    }

    /// Open the database within a transaction.
    pub fn transaction(mut self, txn: &'a Transaction) -> Self {
        self.txn = Some(txn);
        self
    }

    /// Specify the database file.
    pub fn file<P: AsRef<Path>>(mut self, file: P) -> Self {
        self.file = Some(CString::new(file.as_ref().to_str().unwrap()).unwrap());
        self
    }

    /// Specify the database name.
    pub fn name(mut self, name: &str) -> Self {
        self.name = Some(CString::new(name).unwrap());
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
    pub fn open(self) -> Result<Database, Error> {
        // Get the DB_ENV pointer
        let env_ptr = match self.env.as_ref() {
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

        let dbtype = db_ffi::DBTYPE::from(self.db_type);

        unsafe {
            // Create the DB struct
            let mut db: *mut db_ffi::DB = ptr::null_mut();
            let ret = db_ffi::db_create(&mut db, env_ptr, 0);
            if ret != 0 {
                panic!("Could not instantiate DB. errno = {}", ret);
            }

            // Open the database
            let ret = ((*db).open.unwrap())(db, unwrap_txn_ptr(self.txn), file_ptr, database_ptr, dbtype, self.flags.bits(), self.mode);
            match ret {
                0 => Ok(Arc::new(Db { env: self.env, db: db })),
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
/// use libdb::Flags;
///
/// let ret = libdb::DatabaseBuilder::new()
///     .flags(Flags::DB_CREATE)
///     .open();
/// assert!(ret.is_ok())
/// ```
pub struct Db {
    env: Option<Environment>,
    db: *mut db_ffi::DB,
}

impl Db {
    /// Get a key/data pair from the database.
    ///
    /// # Examples
    ///
    /// # Record Found
    /// ```
    /// # use std::str;
    /// use libdb::Flags;
    /// # let db = libdb::DatabaseBuilder::new()
    /// #    .flags(Flags::DB_CREATE)
    /// #    .open()
    /// #    .unwrap();
    /// // Note: BDB requires that the key be mutable.
    /// let mut key   = String::from("key").into_bytes();
    /// let mut value = String::from("value").into_bytes();
    /// assert!(db.put(None, key.as_mut_slice(), value.as_mut_slice(), Flags::DB_NONE).is_ok());
    ///
    /// let ret = db.get(None, key.as_mut_slice(), Flags::DB_NONE);
    /// assert!(ret.is_ok());
    /// assert_eq!("value", str::from_utf8(ret.ok().unwrap().unwrap().as_slice()).unwrap());
    /// ```
    ///
    /// ## Record Not Found
    /// ```
    /// use libdb::Flags;
    /// # let db = libdb::DatabaseBuilder::new()
    /// #    .flags(Flags::DB_CREATE)
    /// #    .open()
    /// #    .unwrap();
    /// // Note: BDB requires that the key be mutable.
    /// let mut key = String::from("key2").into_bytes();
    /// let ret = db.get(None, key.as_mut_slice(), Flags::DB_NONE);
    /// println!("{:?}", ret);
    /// assert!(ret.is_ok());
    /// assert!(ret.unwrap().is_none());
    /// ```
    pub fn get(&self, txn: Option<&Transaction>, key: &mut [u8], flags: Flags) -> Result<Option<DBT>, Error> {
        let mut key_dbt: db_ffi::DBT = Default::default();
        key_dbt.data = key.as_mut_ptr() as *mut ::std::os::raw::c_void;
        key_dbt.size = key.len() as u32;

        let mut data_dbt: db_ffi::DBT = Default::default();
        data_dbt.flags = db_ffi::DB_DBT_MALLOC;

        unsafe {
            match ((*self.db).get.unwrap())(self.db, unwrap_txn_ptr(txn), &mut key_dbt, &mut data_dbt, flags.bits()) {
                0 => Ok(Some(DBT::from(data_dbt))),
                error::DB_NOTFOUND => Ok(None),
                e => Err(Error::new(e))
            }
        }
    }

    /// Store a key/data pair in the database.
    ///
    /// # Examples
    /// ```
    /// use libdb::Flags;
    ///
    /// # let db = libdb::DatabaseBuilder::new()
    /// #    .flags(Flags::DB_CREATE)
    /// #    .open()
    /// #    .unwrap();
    /// // Note: BDB requires that the key and value be mutable.
    /// let mut key   = String::from("key").into_bytes();
    /// let mut value = String::from("value").into_bytes();
    /// let ret = db.put(None, key.as_mut_slice(), value.as_mut_slice(), Flags::DB_NONE);
    /// assert!(ret.is_ok());
    /// ```
    pub fn put(&self, txn: Option<&Transaction>, key: &mut [u8], data: &mut [u8], flags: Flags) -> Result<(), Error> {
        let mut key_dbt: db_ffi::DBT = Default::default();
        key_dbt.data = key.as_mut_ptr() as *mut ::std::os::raw::c_void;
        key_dbt.size = key.len() as u32;

        let mut data_dbt: db_ffi::DBT = Default::default();
        data_dbt.data = data.as_mut_ptr() as *mut ::std::os::raw::c_void;
        data_dbt.size = data.len() as u32;

        unsafe {
            match ((*self.db).put.unwrap())(self.db, unwrap_txn_ptr(txn), &mut key_dbt, &mut data_dbt, flags.bits()) {
                0 => Ok(()),
                e => Err(Error::new(e))
            }
        }
    }

    /// Get a cursor on the database.
    ///
    /// # Examples
    /// ```
    /// use libdb::Flags;
    /// # let db = libdb::DatabaseBuilder::new()
    /// #    .flags(Flags::DB_CREATE)
    /// #    .open()
    /// #    .unwrap();
    /// // Note: BDB requires that the key and value be mutable.
    /// let mut key   = String::from("key").into_bytes();
    /// let mut value = String::from("value").into_bytes();
    /// let ret = db.put(None, key.as_mut_slice(), value.as_mut_slice(), Flags::DB_NONE);
    /// assert!(ret.is_ok());
    ///
    /// // get cursor and iterate
    /// let mut cursor = db.cursor().expect("Failed to get cursor");
    /// ```
    pub fn cursor(&self) -> Result<Cursor, Error> {
        let mut dbc: db_ffi::DBC = db_ffi::DBC::default();
        let mut dbc_ptr: *mut db_ffi::DBC = &mut dbc as *mut db_ffi::DBC;
        unsafe {
            match ((*self.db).cursor.unwrap())(self.db, ptr::null_mut(), &mut dbc_ptr as *mut *mut db_ffi::DBC, 0) {
                0 => Ok(Cursor{dbc_ptr}),
                e => Err(Error::new(e)),
            }
        }
    }
}

pub struct Cursor {
    dbc_ptr: *mut db_ffi::DBC,
}

impl Cursor {
    /// Iterate over key/data pairs in the database.
    ///
    /// # Examples
    /// ```
    /// use libdb::Flags;
    /// # use std::str;
    /// # let db = libdb::DatabaseBuilder::new()
    /// #    .flags(Flags::DB_CREATE)
    /// #    .open()
    /// #    .unwrap();
    /// // Note: BDB requires that the key and value be mutable.
    /// let mut key   = String::from("key").into_bytes();
    /// let mut value = String::from("value").into_bytes();
    /// let ret = db.put(None, key.as_mut_slice(), value.as_mut_slice(), Flags::DB_NONE);
    /// assert!(ret.is_ok());
    ///
    /// // get cursor and iterate
    /// let mut cursor = db.cursor().expect("Failed to get cursor");
    /// let (key_dbt, data_dbt) = cursor.next().expect("Could not walk cursor");
    ///     assert_eq!("key", str::from_utf8(key_dbt.unwrap().as_slice()).unwrap());
    ///     assert_eq!("value", str::from_utf8(data_dbt.unwrap().as_slice()).unwrap());
    /// ```
    pub fn next(&mut self) -> Result<(Option<DBT>, Option<DBT>), Error> {
        let mut key_dbt: db_ffi::DBT = Default::default();
        key_dbt.flags = db_ffi::DB_DBT_MALLOC;

        let mut data_dbt: db_ffi::DBT = Default::default();
        data_dbt.flags = db_ffi::DB_DBT_MALLOC;
        unsafe {
            match ((*self.dbc_ptr).c_get.unwrap())(self.dbc_ptr, &mut key_dbt, &mut data_dbt, db_ffi::DB_NEXT) {
                0 => Ok((Some(DBT::from(key_dbt)), Some(DBT::from(data_dbt)))),
                e => Err(Error::new(e)),
            }
        }
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        unsafe {
            ((*self.db).close.unwrap())(self.db, 0);
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

unsafe impl Send for Env {}
unsafe impl Sync for Env {}
unsafe impl Send for Db {}
unsafe impl Sync for Db {}
