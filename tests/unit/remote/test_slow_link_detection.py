import logging

import colorama
import pytest
from dvc.config import Config
from dvc.remote.local.slow_link_detection import (
    slow_link_guard,
    SlowLinkDetectorDecorator,
)
from mock import Mock, patch


@patch(
    "dvc.remote.local.slow_link_detection.SlowLinkDetectorDecorator",
    autospec=True,
)
class TestSlowLinkGuard(object):
    @pytest.fixture(autouse=True)
    def setUp(self):
        self.method = Mock()
        self.remote_local_mock = Mock()
        self.config_mock = Mock()
        self.remote_local_mock.repo.config.config.get.return_value = (
            self.config_mock
        )

    def _cache_config(self, slow_link_warning, cache_type):
        def config_side_effect(section_name, _):
            if section_name == Config.SECTION_CACHE_SLOW_LINK_WARNING:
                return slow_link_warning
            elif section_name == Config.SECTION_CACHE_TYPE:
                return cache_type

        self.config_mock.get.side_effect = config_side_effect

    def test_should_decorate_on_slow_link_warning_and_no_cache_type(
        self, SlowLinkDetectorDecoratorClassMock
    ):
        self._cache_config(slow_link_warning=True, cache_type=None)

        slow_link_guard(self.method)(self.remote_local_mock)

        assert 1 == SlowLinkDetectorDecoratorClassMock.call_count
        assert 0 == self.method.call_count

    @pytest.mark.parametrize(
        "slow_link_warning, cache_type",
        [(True, "any_cache_type"), (False, None)],
    )
    def test_should_not_decorate(
        self, SlowLinkDetectorDecoratorClassMock, slow_link_warning, cache_type
    ):
        self._cache_config(
            slow_link_warning=slow_link_warning, cache_type=cache_type
        )

        slow_link_guard(self.method)(self.remote_local_mock)

        assert 0 == SlowLinkDetectorDecoratorClassMock.call_count
        assert 1 == self.method.call_count


class TestSlowLinkDetectorDecorator(object):
    @pytest.fixture(autouse=True)
    def setUp(self):
        self.args_mock = Mock()
        self.method_result_mock = Mock()

        def method_effect(arg):
            assert self.args_mock == arg
            return self.method_result_mock

        self.method_mock = Mock(side_effect=method_effect)

    @patch.object(SlowLinkDetectorDecorator, "LINKING_TIMEOUT_SECONDS", 0.0)
    def test_should_log_only_once(self, caplog):

        with caplog.at_level(logging.WARNING, logger="dvc"):
            test_instance = SlowLinkDetectorDecorator(self.method_mock)
            result = test_instance(self.args_mock)
            assert self.method_result_mock == result

            # slow link guard keeps creating new SlowLinkDetectorDecorators
            # each time it is called, so we need to make sure that we are
            # preventing every new instance of this class from displaying the
            # message
            test_instance2 = SlowLinkDetectorDecorator(self.method_mock)
            result = test_instance2(self.args_mock)
            assert self.method_result_mock == result

        msg = (
            "You can cut execution time considerably. Check:\n"
            "{blue}https://dvc.org/doc/commands-reference/config#cache{"
            "reset}"
            "\nfor "
            "more information.\nTo disable this message, run:\n'dvc "
            "config "
            "cache.slow_link_warning False'".format(
                blue=colorama.Fore.BLUE, reset=colorama.Fore.RESET
            )
        )
        assert 1 == len(caplog.messages)
        assert msg == caplog.messages[0]

    @patch.object(
        SlowLinkDetectorDecorator, "LINKING_TIMEOUT_SECONDS", 99999.0
    )
    def test_should_not_log(self, caplog):
        test_instance = SlowLinkDetectorDecorator(self.method_mock)

        with caplog.at_level(logging.WARNING, logger="dvc"):
            result = test_instance(self.args_mock)

        assert self.method_result_mock == result
        assert 0 == len(caplog.messages)
