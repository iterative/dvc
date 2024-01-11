import os

from funcy import get_in

from dvc.cli import formatter
from dvc.cli.utils import append_doc_link
from dvc.commands.config import CmdConfig
from dvc.log import logger

logger = logger.getChild(__name__)


class CmdStudioLogin(CmdConfig):
    def run(self):
        from dvc_studio_client.auth import StudioAuthError, get_access_token

        from dvc.env import DVC_STUDIO_URL
        from dvc.ui import ui
        from dvc.utils.studio import STUDIO_URL

        name = self.args.name
        hostname = self.args.hostname or os.environ.get(DVC_STUDIO_URL) or STUDIO_URL
        scopes = self.args.scopes

        try:
            token_name, access_token = get_access_token(
                token_name=name,
                hostname=hostname,
                scopes=scopes,
                use_device_code=self.args.no_open,
                client_name="DVC",
            )
        except StudioAuthError as e:
            ui.error_write(str(e))
            return 1

        self.save_config(hostname, access_token)
        ui.write(
            "Authentication has been successfully completed."
            "The generated token will now be accessible as"
            f" {token_name} in the user's Studio profile."
        )
        return 0

    def save_config(self, hostname, token):
        with self.config.edit("global") as conf:
            conf["studio"]["token"] = token
            conf["studio"]["url"] = hostname


class CmdStudioLogout(CmdConfig):
    def run(self):
        from dvc.ui import ui

        with self.config.edit("global") as conf:
            if not get_in(conf, ["studio", "token"]):
                ui.error_write("Not logged in to Studio.")
                return 1

            del conf["studio"]["token"]

        ui.write("Logged out from Studio")
        return 0


class CmdStudioToken(CmdConfig):
    def run(self):
        from dvc.ui import ui

        conf = self.config.read("global")
        token = get_in(conf, ["studio", "token"])
        if not token:
            ui.error_write("Not logged in to Studio.")
            return 1

        ui.write(token)
        return 0


def add_parser(subparsers, parent_parser):
    STUDIO_HELP = "Commands to authenticate DVC with Iterative Studio"
    STUDIO_DESCRIPTION = (
        "Authenticate DVC with Studio and set the token."
        " Once this token has been properly configured,\n"
        " DVC will utilize it for seamlessly sharing live experiments\n"
        " and sending notifications to Studio regarding any experiments"
        " that have been pushed."
    )

    studio_parser = subparsers.add_parser(
        "studio",
        parents=[parent_parser],
        description=append_doc_link(STUDIO_DESCRIPTION, "studio"),
        help=STUDIO_HELP,
        formatter_class=formatter.RawDescriptionHelpFormatter,
    )
    studio_subparser = studio_parser.add_subparsers(
        dest="cmd",
        help="Use `DVC studio CMD --help` to display command-specific help.",
        required=True,
    )

    STUDIO_LOGIN_HELP = "Authenticate DVC with Studio host"
    STUDIO_LOGIN_DESCRIPTION = (
        "By default, this command authenticates the DVC with Studio\n"
        " using default scopes and assigns a random name as the token name."
    )
    login_parser = studio_subparser.add_parser(
        "login",
        parents=[parent_parser],
        description=append_doc_link(STUDIO_LOGIN_DESCRIPTION, "studio/login"),
        help=STUDIO_LOGIN_HELP,
        formatter_class=formatter.RawDescriptionHelpFormatter,
    )

    login_parser.add_argument(
        "-H",
        "--hostname",
        action="store",
        default=None,
        help="The hostname of the Studio instance to authenticate with.",
    )
    login_parser.add_argument(
        "-s",
        "--scopes",
        action="store",
        default=None,
        help="The scopes for the authentication token. ",
    )

    login_parser.add_argument(
        "-n",
        "--name",
        action="store",
        default=None,
        help="The name of the authentication token. It will be used to\n"
        "identify token shown in Studio profile.",
    )

    login_parser.add_argument(
        "--no-open",
        action="store_true",
        default=False,
        help="Use authentication flow based on user code.\n"
        "You will be presented with user code to enter in browser.\n"
        "DVC will also use this if it cannot launch browser on your behalf.",
    )
    login_parser.set_defaults(func=CmdStudioLogin)

    STUDIO_LOGOUT_HELP = "Logout user from Studio"
    STUDIO_LOGOUT_DESCRIPTION = (
        "This removes the studio token from your global config.\n"
    )

    logout_parser = studio_subparser.add_parser(
        "logout",
        parents=[parent_parser],
        description=append_doc_link(STUDIO_LOGOUT_DESCRIPTION, "studio/logout"),
        help=STUDIO_LOGOUT_HELP,
        formatter_class=formatter.RawDescriptionHelpFormatter,
    )

    logout_parser.set_defaults(func=CmdStudioLogout)

    STUDIO_TOKEN_HELP = "View the token dvc uses to contact Studio"  # noqa: S105 # nosec B105

    logout_parser = studio_subparser.add_parser(
        "token",
        parents=[parent_parser],
        description=append_doc_link(STUDIO_TOKEN_HELP, "studio/token"),
        help=STUDIO_TOKEN_HELP,
        formatter_class=formatter.RawDescriptionHelpFormatter,
    )

    logout_parser.set_defaults(func=CmdStudioToken)
