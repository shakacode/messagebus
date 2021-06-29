## MewssageBus changelog

### 0.6.5
#### new features:
* the `Message` trait no more required to be `Clone`
* added methods `send_one`, `send_one_blocked`, `try_send_one` which does not require `Message: Clone`

### 0.6.4
#### new features:
* added struct `Module` and `BusBuilder::add_module` 

### 0.6.3
#### fixes:
* Update Error Handling

#### breaking changes:
* all methods now return `messagenus::Error`

### 0.6.2 
#### new features:
* add `request_we` for requests when we know the handler's error type 

#### fixes:
* Update Error Handling

#### breaking changes:
* send methods now return `messagenus::SendError`

#### notes:
* Got rid of `anyhow::Error`

### 0.6.0 
#### new features:
* Request/Response API (request method)

#### fixes:
* Fix some unwaked receivers

#### breaking changes:
* Add `type Response` and `type Error` into all handlers

#### notes:
* Refactorin Receivers API