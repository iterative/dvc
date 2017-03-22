nosetests --cover-inclusive --cover-erase --cover-package=dvc --with-coverage

if [ "$1" = "report" ]; then
    codeclimate-test-reporter
fi
