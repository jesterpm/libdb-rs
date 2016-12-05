#![allow(dead_code)]

use std::ffi::CStr;
use std::fmt;
use db_ffi;

/// An error returned from a BDB library call.
#[derive(Debug)]
pub struct Error {
    errno: i32,
}

impl Error {
    /// Create a new `Error`.
    pub fn new(errno: i32) -> Error {
        Error { errno: errno }
    }

    /// Return the error number.
    pub fn errno(&self) -> i32 {
        self.errno
    }

    /// Return a `String` describing the error.
    pub fn as_string(&self) -> String {
        unsafe {
            CStr::from_ptr(db_ffi::db_strerror(self.errno)).to_string_lossy().into_owned()
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.as_string())
    }
}

/// User memory too small for return.
pub const DB_BUFFER_SMALL: i32      = -30999;
/// "Null" return from 2ndary callbk.
pub const DB_DONOTINDEX: i32        = -30998;
/// A foreign db constraint triggered.
pub const DB_FOREIGN_CONFLICT: i32  = -30997;
/// Key/data deleted or never created.
pub const DB_KEYEMPTY: i32          = -30996;
/// The key/data pair already exists.
pub const DB_KEYEXIST: i32          = -30995;
/// Deadlock.
pub const DB_LOCK_DEADLOCK: i32     = -30994;
/// Lock unavailable.
pub const DB_LOCK_NOTGRANTED: i32   = -30993;
/// In-memory log buffer full.
pub const DB_LOG_BUFFER_FULL: i32   = -30992;
/// Server panic return.
pub const DB_NOSERVER: i32          = -30991;
/// Bad home sent to server.
pub const DB_NOSERVER_HOME: i32     = -30990;
/// Bad ID sent to server.
pub const DB_NOSERVER_ID: i32       = -30989;
/// Key/data pair not found (EOF).
pub const DB_NOTFOUND: i32          = -30988;
/// Out-of-date version.
pub const DB_OLD_VERSION: i32       = -30987;
/// Requested page not found.
pub const DB_PAGE_NOTFOUND: i32     = -30986;
/// There are two masters.
pub const DB_REP_DUPMASTER: i32     = -30985;
/// Rolled back a commit.
pub const DB_REP_HANDLE_DEAD: i32   = -30984;
/// Time to hold an election.
pub const DB_REP_HOLDELECTION: i32  = -30983;
/// This msg should be ignored
pub const DB_REP_IGNORE: i32        = -30982;
/// Cached not written perm written
pub const DB_REP_ISPERM: i32        = -30981;
/// Unable to join replication group.
pub const DB_REP_JOIN_FAILURE: i32  = -30980;
/// Master lease has expired.
pub const DB_REP_LEASE_EXPIRED: i32 = -30979;
/// API/Replication lockout now.
pub const DB_REP_LOCKOUT: i32       = -30978;
/// New site entered system.
pub const DB_REP_NEWSITE: i32       = -30977;
/// Permanent log record not written.
pub const DB_REP_NOTPERM: i32       = -30976;
/// Site cannot currently be reached.
pub const DB_REP_UNAVAIL: i32       = -30975;
/// Panic return.
pub const DB_RUNRECOVERY: i32       = -30974;
/// Secondary index corrupt.
pub const DB_SECONDARY_BAD: i32     = -30973;
/// Verify failed; bad format.
pub const DB_VERIFY_BAD: i32        = -30972;
/// Environment version mismatch.
pub const DB_VERSION_MISMATCH: i32  = -30971;