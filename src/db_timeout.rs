use crate::AppError;
use std::future::Future;
use std::time::Duration;

/// Wraps a database query future with a timeout
pub async fn with_timeout<F, T>(timeout: Duration, future: F) -> Result<T, AppError>
where
    F: Future<Output = Result<T, sqlx::Error>>,
{
    match tokio::time::timeout(timeout, future).await {
        Ok(Ok(result)) => Ok(result),
        Ok(Err(e)) => Err(AppError::InternalError(e.into())),
        Err(_) => Err(AppError::RequestTimeout(format!(
            "Database query exceeded timeout of {:?}",
            timeout
        ))),
    }
}
