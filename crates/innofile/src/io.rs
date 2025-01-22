#[cfg(feature = "sync")]
pub use crate::sync::io::Closeable as SyncCloseable;
#[cfg(feature = "tokio")]
pub use crate::tokio::io::Closeable as AsyncCloseable;
