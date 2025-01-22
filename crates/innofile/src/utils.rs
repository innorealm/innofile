use std::path::Path;

use fluent_uri::UriRef;

use crate::error::InnoFileResult;

pub fn path_extension(path: impl AsRef<str>) -> InnoFileResult<Option<String>> {
    Ok(Path::new(UriRef::parse(path.as_ref())?.path().as_str())
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.to_string()))
}
