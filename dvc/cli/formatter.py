import argparse


class HelpFormatter(argparse.HelpFormatter):
    def _get_default_metavar_for_optional(self, action: argparse.Action) -> str:
        return action.dest


class RawTextHelpFormatter(HelpFormatter, argparse.RawTextHelpFormatter):
    pass


class RawDescriptionHelpFormatter(HelpFormatter, argparse.RawDescriptionHelpFormatter):
    pass
