# Improve SSH Error Messages for Permission vs File Not Found Issues

## Description
This PR addresses issue #7861 by improving error handling for SSH operations in the DVC codebase. Currently, when users encounter permission issues with SSH remotes, they incorrectly receive "No such file or directory" errors, which is confusing and makes troubleshooting difficult.

The implementation correctly identifies and maps Paramiko SFTP error codes to appropriate exception types, providing clear, descriptive error messages that distinguish between permission problems and missing files.

## Changes
- Enhanced error handling in SSH filesystem operations (exists, get, put, remove)
- Added proper error mapping from SFTP error codes to appropriate exceptions
- Improved error messages with context-specific information
- Added unit tests to verify the improved error handling

## Related Issues
Fixes #7861

## Checklist
* [x] ❗ I have followed the [Contributing to DVC](https://dvc.org/doc/user-guide/contributing/core) checklist.

* [x] 📖 Documentation update: Since this is a bug fix that improves error messages without changing the API, a separate documentation PR isn't required. However, it might be beneficial to update the troubleshooting guide to mention the improved error messages for SSH remotes.

Thank you for the contribution - we'll try to review it as soon as possible. 🙏
