#!/bin/bash

set -e

source common.sh

function test_generic() {
	dvc_info '=== Generic repro test'

	dvc_create_repo

	dvc_info 'Copy foo into foo1'
	dvc add data/foo
	dvc run -d code/code.sh -d data/foo -o data/foo1 bash code/code.sh data/foo data/foo1

	dvc_info 'Modify code'
	echo " " >> code/code.sh 
	git commit -am 'Change code'
	    
	dvc_info 'Reproduce foo1'
	dvc repro foo1.dvc
	dvc_check_files data/foo1 data/foo1.dvc
	if [ "$(cat data/foo1)" != "foo" ]; then
		dvc_fail
	fi

	dvc_info 'Modify foo'
	rm -f data/foo
	cp $DATA_CACHE/bar data/foo

	dvc_info 'Reproduce foo1 as default target'
	dvc repro foo1.dvc
	dvc_check_files data/foo1 data/foo1.dvc
	if [ "$(cat data/foo1)" != "bar" ]; then
		dvc_fail
	fi
}

function test_partial() {
	dvc_info "=== Partial repro test"

	dvc_create_repo

	cp code/code.sh code/code1.sh
	cp code/code1.sh code/code2.sh
	cp code/code2.sh code/code3.sh
	git add code
	git commit -m "copy code"

	dvc_info "Create repro chain foo -> foo1 -> foo2 -> foo3"
	dvc add data/foo
	dvc run -f copy_foo_foo1.dvc -d code/code1.sh -d data/foo -o data/foo1 cp data/foo data/foo1
	dvc run -f copy_foo1_foo2.dvc -d code/code2.sh -d data/foo1 -o data/foo2 cp data/foo1 data/foo2
	dvc run -f copy_foo2_foo3.dvc -d code/code3.sh -d data/foo2 -o data/foo3 cp data/foo2 data/foo3

	dvc_info "Save original timestamps"
	FOO_TS=$(dvc_timestamp data/foo)
	FOO1_TS=$(dvc_timestamp data/foo1)
	FOO2_TS=$(dvc_timestamp data/foo2)
	FOO3_TS=$(dvc_timestamp data/foo3)

	dvc_info "Modify dependency for data/foo1, that will not result in data/foo1 contents being changed"
	echo " " >> code/code1.sh
	git add code
	git commit -m "modify code1.sh"

	dvc_info "Reproduce data/foo3"
	dvc repro copy_foo2_foo3.dvc

	dvc_info "Check timestamps"
	if [ "$FOO_TS" != "$(dvc_timestamp data/foo)" ]; then
		dvc_error "data/foo timestamp changed"
		dvc_fail
	fi

	if [ "$FOO1_TS" != "$(dvc_timestamp data/foo1)" ]; then
		dvc_error "data/foo1 timestamp changed"
		dvc_fail
	fi

	if [ "$FOO2_TS" != "$(dvc_timestamp data/foo2)" ]; then
		dvc_error "data/foo2 timestamp changed"
		dvc_fail
	fi

	if [ "$FOO3_TS" != "$(dvc_timestamp data/foo3)" ]; then
		dvc_error "data/foo3 timestamp changed"
		dvc_fail
	fi
}

test_generic
test_partial

dvc_pass
