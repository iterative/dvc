# SSH Error Message Improvements

## Bug Description

This bug fix addresses issue #7861 where SSH remotes show misleading error messages when permission problems occur. Currently, permission issues incorrectly display as "No such file or directory" errors, which leads to confusion during troubleshooting.

## Solution

The solution improves error handling in the SSH filesystem implementation by:

1. Correctly identifying and mapping Paramiko SFTP error codes to appropriate exceptions
2. Adding clear, descriptive error messages that specify permission-related problems
3. Maintaining consistent error handling across all SSH operations (exists, get, put, remove)

## Files Changed

1. `dvc_ssh/fs.py` - The main implementation of the SSH filesystem
2. Added new unit tests in `tests/unit/fs/test_ssh_error_messages.py`

## Tests

The tests are designed to work in any environment, even without the Paramiko dependency. They use mocks to simulate the SSH operations and verify the correct error messages are generated.

Key test scenarios:
1. File not found errors are correctly reported
2. Permission denied errors include clear messages about permissions
3. Different operations (read/write) include operation-specific error messages

## How to Run Tests

```bash
./run.sh tests/unit/fs/test_ssh_error_messages.py
```

## Implementation Notes

The changes are backward compatible and work with both the built-in Python exceptions and DVC's exception hierarchy. The tests are designed to be resilient to environment differences and dependency availability.

## Impact

These changes significantly improve the user experience when working with SSH remotes by providing clear, actionable error messages rather than misleading ones. Users will now be able to quickly identify and fix permission-related issues when using commands like `dvc push`, `dvc pull`, `dvc get`, etc. with SSH remotes.
