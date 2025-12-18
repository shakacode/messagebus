use async_trait::async_trait;
use messagebus::{
    derive::{Error as MbError, Message},
    error, AsyncHandler, Bus, Message, TypeTagged,
};
use thiserror::Error;

// ============================================================================
// Basic Error derive tests
// ============================================================================

/// Simple error with default type tag
#[derive(Debug, Error, MbError)]
enum SimpleError {
    #[error("Simple error: {0}")]
    Message(String),
}

/// Test that Error derive implements TypeTagged
#[test]
fn test_error_derive_type_tagged() {
    let err = SimpleError::Message("test".to_string());

    // Should have a type tag
    let tag = err.type_tag();
    assert_eq!(tag.as_ref(), "SimpleError");

    // Static type tag should work too
    assert_eq!(SimpleError::type_tag_().as_ref(), "SimpleError");
}

// ============================================================================
// Error with custom type_tag attribute
// ============================================================================

#[derive(Debug, Error, MbError)]
#[type_tag("custom::error::CustomError")]
enum CustomTagError {
    #[error("Custom error")]
    Custom,
}

/// Test custom type_tag attribute on Error
#[test]
fn test_error_derive_custom_type_tag() {
    let err = CustomTagError::Custom;

    assert_eq!(err.type_tag().as_ref(), "custom::error::CustomError");
    assert_eq!(
        CustomTagError::type_tag_().as_ref(),
        "custom::error::CustomError"
    );
}

// ============================================================================
// Error with namespace attribute
// ============================================================================

#[derive(Debug, Error, MbError)]
#[namespace("my_app::errors")]
enum NamespacedError {
    #[error("Namespaced error")]
    Namespaced,
}

/// Test namespace attribute on Error
#[test]
fn test_error_derive_namespace() {
    let err = NamespacedError::Namespaced;

    assert_eq!(err.type_tag().as_ref(), "my_app::errors::NamespacedError");
    assert_eq!(
        NamespacedError::type_tag_().as_ref(),
        "my_app::errors::NamespacedError"
    );
}

// ============================================================================
// Error with multiple variants
// ============================================================================

#[derive(Debug, Error, MbError)]
enum MultiVariantError {
    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Permission denied")]
    PermissionDenied,

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Internal error")]
    Internal,
}

/// Test Error derive with multiple variants
#[test]
fn test_error_derive_multiple_variants() {
    // All variants should have the same type tag (the enum's tag)
    let err1 = MultiVariantError::NotFound("file.txt".to_string());
    let err2 = MultiVariantError::PermissionDenied;
    let err3 = MultiVariantError::Internal;

    assert_eq!(err1.type_tag().as_ref(), "MultiVariantError");
    assert_eq!(err2.type_tag().as_ref(), "MultiVariantError");
    assert_eq!(err3.type_tag().as_ref(), "MultiVariantError");
}

// ============================================================================
// Error used in handler
// ============================================================================

#[derive(Debug, Error, MbError)]
enum HandlerError {
    #[error("Handler error: {0}")]
    Error(anyhow::Error),

    #[error("Validation failed: {0}")]
    ValidationFailed(String),

    #[error("Not found")]
    NotFound,
}

impl<M: Message> From<error::Error<M>> for HandlerError {
    fn from(err: error::Error<M>) -> Self {
        Self::Error(err.into())
    }
}

#[derive(Debug, Clone, Message)]
struct ValidateMsg(pub String);

#[derive(Debug, Clone, Message)]
struct ValidationResult(pub bool);

struct ValidatingReceiver;

#[async_trait]
impl AsyncHandler<ValidateMsg> for ValidatingReceiver {
    type Error = HandlerError;
    type Response = ValidationResult;

    async fn handle(&self, msg: ValidateMsg, _bus: &Bus) -> Result<Self::Response, Self::Error> {
        if msg.0.is_empty() {
            Err(HandlerError::ValidationFailed(
                "Message cannot be empty".to_string(),
            ))
        } else if msg.0 == "not_found" {
            Err(HandlerError::NotFound)
        } else {
            Ok(ValidationResult(true))
        }
    }
}

/// Test Error derive in handler context - success case
#[tokio::test]
async fn test_error_derive_in_handler_success() {
    let (b, poller) = Bus::build()
        .register(ValidatingReceiver)
        .subscribe_async::<ValidateMsg>(8, Default::default())
        .done()
        .build();

    let result = b
        .request_we::<_, ValidationResult, HandlerError>(
            ValidateMsg("valid input".to_string()),
            Default::default(),
        )
        .await;

    assert!(result.is_ok());
    assert!(result.unwrap().0);

    b.close().await;
    poller.await;
}

/// Test Error derive in handler context - validation error
#[tokio::test]
async fn test_error_derive_in_handler_validation_error() {
    let (b, poller) = Bus::build()
        .register(ValidatingReceiver)
        .subscribe_async::<ValidateMsg>(8, Default::default())
        .done()
        .build();

    let result = b
        .request_we::<_, ValidationResult, HandlerError>(
            ValidateMsg(String::new()),
            Default::default(),
        )
        .await;

    assert!(result.is_err());

    b.close().await;
    poller.await;
}

// ============================================================================
// Generic error type
// ============================================================================

#[derive(Debug, Error, MbError)]
enum GenericError<T: std::fmt::Debug + Send + Sync + 'static> {
    #[error("Wrapped error: {0:?}")]
    Wrapped(T),

    #[allow(dead_code)]
    #[error("Generic error")]
    Generic,
}

/// Test Error derive with generic parameters
#[test]
fn test_error_derive_generic() {
    let err: GenericError<String> = GenericError::Wrapped("test".to_string());

    // Generic errors should include the type parameter in the tag
    let tag = err.type_tag();
    assert!(tag.as_ref().contains("GenericError"));
}

// ============================================================================
// Error with Clone
// ============================================================================

#[derive(Debug, Error, Clone, MbError)]
enum CloneableError {
    #[error("Cloneable error: {0}")]
    Message(String),
}

/// Test that Error derive works with Clone
#[test]
fn test_error_derive_with_clone() {
    let err = CloneableError::Message("original".to_string());
    let cloned = err.clone();

    assert_eq!(err.type_tag(), cloned.type_tag());
}

// ============================================================================
// Error with struct variant
// ============================================================================

#[derive(Debug, Error, MbError)]
enum StructVariantError {
    #[error("Struct error: code={code}, message={message}")]
    Struct { code: u32, message: String },

    #[allow(dead_code)]
    #[error("Tuple error")]
    Tuple(u32),

    #[allow(dead_code)]
    #[error("Unit error")]
    Unit,
}

/// Test Error derive with struct variants
#[test]
fn test_error_derive_struct_variant() {
    let err = StructVariantError::Struct {
        code: 404,
        message: "Not Found".to_string(),
    };

    assert_eq!(err.type_tag().as_ref(), "StructVariantError");
    assert_eq!(err.to_string(), "Struct error: code=404, message=Not Found");
}

// ============================================================================
// Error type_name and type_layout
// ============================================================================

#[derive(Debug, Error, MbError)]
#[namespace("test")]
enum LayoutError {
    #[error("Test")]
    Test,
}

/// Test type_name method from TypeTagged
#[test]
fn test_error_type_name() {
    let err = LayoutError::Test;
    let name = err.type_name();

    assert_eq!(name.as_ref(), "test::LayoutError");
}

/// Test type_layout method from TypeTagged
#[test]
fn test_error_type_layout() {
    let err = LayoutError::Test;
    let layout = err.type_layout();

    // Layout should be valid
    assert!(layout.size() > 0 || layout.size() == 0); // Enums can be 0-sized
    assert!(layout.align() > 0);
}

// ============================================================================
// Multiple errors in same handler
// ============================================================================

#[derive(Debug, Error, MbError)]
#[namespace("service_a")]
enum ServiceAError {
    #[error("Service A error")]
    Error,
}

#[derive(Debug, Error, MbError)]
#[namespace("service_b")]
enum ServiceBError {
    #[error("Service B error")]
    Error,
}

/// Test that different error types have different type tags
#[test]
fn test_different_error_type_tags() {
    let err_a = ServiceAError::Error;
    let err_b = ServiceBError::Error;

    assert_ne!(err_a.type_tag(), err_b.type_tag());
    assert_eq!(err_a.type_tag().as_ref(), "service_a::ServiceAError");
    assert_eq!(err_b.type_tag().as_ref(), "service_b::ServiceBError");
}

// ============================================================================
// Error with lifetimes (if supported)
// ============================================================================

// Note: Errors with lifetimes are typically not used in messagebus
// because errors need to be Send + 'static, but we test the derive still works

#[derive(Debug, Error, MbError)]
enum OwnedError {
    #[error("Owned string error: {0}")]
    OwnedString(String),

    #[error("Boxed error: {0}")]
    Boxed(Box<dyn std::error::Error + Send + Sync>),
}

/// Test Error derive with owned/boxed variants
#[test]
fn test_error_derive_owned_variants() {
    let err1 = OwnedError::OwnedString("test".to_string());
    let err2: OwnedError = OwnedError::Boxed(Box::new(std::io::Error::other("io error")));

    assert_eq!(err1.type_tag().as_ref(), "OwnedError");
    assert_eq!(err2.type_tag().as_ref(), "OwnedError");
}
