#![allow(clippy::len_without_is_empty)]

pub mod arrow;
pub mod error;
pub mod fs;
pub mod io;
#[cfg(feature = "sync")]
pub mod sync;
#[cfg(feature = "tokio")]
pub mod tokio;
pub mod utils;

macro_rules! with_field {
    ($method:ident, $field:ident, String) => {
        pub fn $method(mut self, $field: Option<impl ToString>) -> Self {
            self.$field = $field.map(|$field| $field.to_string());
            self
        }
    };

    ($method:ident, $field:ident, $type:ident) => {
        pub fn $method(mut self, $field: Option<$type>) -> Self {
            self.$field = $field;
            self
        }
    };
}

pub(crate) use with_field;
