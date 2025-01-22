use async_trait::async_trait;

use crate::error::InnoFileResult;

/// The supertrait of [`Closeable`] to close on `Box<Self>`.
#[async_trait]
pub trait CloseableBoxed {
    async fn close_boxed(self: Box<Self>) -> InnoFileResult<()>;
}

/// A trait for objects to close on `Self`.
#[async_trait]
pub trait Closeable: CloseableBoxed + Send {
    async fn close(self) -> InnoFileResult<()>;
}

// Default implementation of [`CloseableBoxed`] trait for [`Closeable`] objects.
#[async_trait]
impl<T: Closeable> CloseableBoxed for T {
    async fn close_boxed(self: Box<Self>) -> InnoFileResult<()> {
        // Dereferencing `Box<Self>` with `*self` to close on `Self`.
        (*self).close().await
    }
}

// Implement [`Closeable`] trait for its `Box`ed types.
#[async_trait]
impl<T: Closeable + ?Sized> Closeable for Box<T> {
    async fn close(self) -> InnoFileResult<()> {
        self.close_boxed().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Closer;

    #[async_trait]
    impl Closeable for Closer {
        async fn close(self) -> InnoFileResult<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_closeable() -> InnoFileResult<()> {
        let closer = Closer;
        assert!(closer.close().await.is_ok());

        let boxed_closer = Box::new(Closer) as Box<dyn Closeable>;
        assert!(boxed_closer.close().await.is_ok());

        Ok(())
    }
}
