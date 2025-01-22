use crate::error::InnoFileResult;

/// The supertrait of [`Closeable`] to close on `Box<Self>`.
pub trait CloseableBoxed {
    fn close_boxed(self: Box<Self>) -> InnoFileResult<()>;
}

/// A trait for objects to close on `Self`.
pub trait Closeable: CloseableBoxed {
    fn close(self) -> InnoFileResult<()>;
}

// Default implementation of [`CloseableBoxed`] trait for [`Closeable`] objects.
impl<T: Closeable> CloseableBoxed for T {
    fn close_boxed(self: Box<Self>) -> InnoFileResult<()> {
        // Dereferencing `Box<Self>` with `*self` to close on `Self`.
        (*self).close()
    }
}

// Implement [`Closeable`] trait for its `Box`ed types.
impl<T: Closeable + ?Sized> Closeable for Box<T> {
    fn close(self) -> InnoFileResult<()> {
        self.close_boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Closer;

    impl Closeable for Closer {
        fn close(self) -> InnoFileResult<()> {
            Ok(())
        }
    }

    #[test]
    fn test_closeable() -> InnoFileResult<()> {
        let closer = Closer;
        assert!(closer.close().is_ok());

        let boxed_closer = Box::new(Closer) as Box<dyn Closeable>;
        assert!(boxed_closer.close().is_ok());

        Ok(())
    }
}
