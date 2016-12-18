use std::slice;
use std::ops::Deref;
use libc;
use libc::c_void;
use std::fmt;

use libdb_sys::ffi;

pub enum DBT<'a> {
    Owned(&'a [u8]),
    Ptr(ffi::DBT),
}

// impl<'a> DBT<'a> {

//     fn as_bytes(&self) -> [u8] {
//         *self
//     }
// }

impl<'a> DBT<'a> {

    pub fn as_slice(&self) -> &[u8] {
        match self {
            &DBT::Owned(s) => s,
            &DBT::Ptr(ptr) => unsafe { slice::from_raw_parts(ptr.data as *const u8, ptr.size as usize) }
        }
    }
}

impl<'a> fmt::Debug for DBT<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self.as_slice())
    }
}

impl<'a> Drop for DBT<'a> {
    fn drop(&mut self) {
        if let &mut DBT::Ptr(ptr) = self {
            unsafe { libc::free(ptr.data as *mut c_void); }
        }
    }
}

impl<'a> Deref for DBT<'a> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl<'a> From<ffi::DBT> for DBT<'a> {
    fn from(ptr: ffi::DBT) -> Self {
        DBT::Ptr(ptr)
    }
}



// / A database thang
// /
// / 
// pub struct DBT<'a> {
//     /// Pointer to the data.
//     data: *mut u8,

//     // Length of the data.
//     size: u32,

//     // Length of a user-supplied buffer.
//     ulen: u32,

//     // dlen and doff are used by BDB for partial reads/updates
//     dlen: u32,
//     doff: u32,
// }

// impl DBT {

//     pub fn from_raw_parts(data: *mut u8, len: u32) -> DBT {
//         DBT {
//             data: data,
//             size: len,
//             ulen: len,
//             dlen: 0,
//             doff: 0,
//         }
//     }

//     pub fn len(&self) -> u32 {
//         self.size
//     }

//     pub fn as_mut_ptr(&mut self) -> *mut u8 {
//         self.data
//     }
// }

// impl From<&mut [u8]> for DBT {
//     fn from(data: &mut [u8]) -> Self {

//     }
// }

// impl Deref for DBT {
//     type Target = [u8];

//     fn deref(&self) -> &[u8] {
//         unsafe { slice::from_raw_parts(self.data, self.size as usize) }
//     }
// }

// impl Drop for DBT {
//     fn drop(&mut self) {
//         unsafe {
//             libc::free(self.data as *mut c_void);
//         }
//     }
// }
