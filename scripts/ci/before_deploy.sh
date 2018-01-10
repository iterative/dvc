if [[ "$TRAVIS_EVENT_TYPE" == "cron" ]]; then
	git config --local user.name 'Nightly Travis Builds'
	git config --local user.email '<>'
	git tag "$(date +'%Y%m%d%H%M%S')-$(git log --format=%h -1)"
fi
