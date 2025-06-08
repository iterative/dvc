# SSH Error Message Improvement Rubric

## Objective
Improve error handling for SSH remotes to provide clear, accurate error messages distinguishing between "permission denied" and "file not found" errors.

## Code Changes
- Created unit tests for SSH filesystem error handling
- Tests mock the SSH filesystem and verify error messages
- Improved error handling without external dependencies
- Added proper documentation

## Key Concepts
- Error handling and reporting
- SSH remote operations
- Testing with mocks
- Improving diagnostics and user experience

## Passing Test Criteria
- All tests pass successfully
- Tests verify that permission errors are correctly identified
- Tests verify that file not found errors are correctly identified
- Tests are resilient to environment differences

## Points Allocation
- Correct error identification: 40%
- Clear error messages: 30%
- Comprehensive test coverage: 20%
- Documentation quality: 10%
