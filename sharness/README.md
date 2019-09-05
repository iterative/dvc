## Running Tests

Tests can be run with:
- `prove *.t`
- `./run.sh *.t`
- `make *.t`

The pattern `*.t` can be omitted, or can be any shell globing pattern,
or a list of test scripts. A single test script can also be executed
by itself, like this: `./t0001-dvc.sh`.

## Naming Tests

**Note:** These naming guidelines are not yet final and may change.

The test files are named like: `tNNNN-commandname-details.t` where N
is a decimal digit.

First digit tells the category:

	0 - the absolute basics and global stuff
	1 - commands about data versioning
	2 - commands about remotes
	3 - commands about pipelines
	4 - commands about metrics
	5 - commands about importing

Second digit tells the particular command we are testing.

Third digit (optionally) tells the particular switch or group of
switches we are testing.

## About Sharness

Sharness is a portable shell library to write, run, and analyze automated tests
for Unix programs. Since all tests output TAP, the [Test Anything Protocol],
they can be run with any TAP harness.

Each test is written as a shell script, for example:

```sh
#!/bin/sh

test_description="Show basic features of Sharness"

. ./sharness.sh

test_expect_success "Success is reported like this" "
    echo hello world | grep hello
"

test_expect_success "Commands are chained this way" "
    test x = 'x' &&
    test 2 -gt 1 &&
    echo success
"

return_42() {
    echo "Will return soon"
    return 42
}

test_expect_success "You can test for a specific exit code" "
    test_expect_code 42 return_42
"

test_expect_failure "We expect this to fail" "
    test 1 = 2
"

test_done
```

For more details see [API.md](https://github.com/chriscool/sharness/blob/master/API.md).

Running the above test script returns the following (TAP) output:

    $ ./simple.t
    ok 1 - Success is reported like this
    ok 2 - Commands are chained this way
    ok 3 - You can test for a specific exit code
    not ok 4 - We expect this to fail # TODO known breakage
    # still have 1 known breakage(s)
    # passed all remaining 3 test(s)
    1..4

Alternatively, you can run the test through [prove(1)]:

    $ prove simple.t
    simple.t .. ok
    All tests successful.
    Files=1, Tests=4,  0 wallclock secs ( 0.02 usr +  0.00 sys =  0.02 CPU)
    Result: PASS

Sharness was derived from the [Git] project - see
[README.git](https://github.com/chriscool/sharness/blob/master/README.git)
for the original documentation.
