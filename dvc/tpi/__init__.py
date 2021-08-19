import logging
import os

from jinja2 import Environment, FileSystemLoader, select_autoescape

logger = logging.getLogger(__name__)

TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "templates")
TEMPLATE_MAIN = "main.tf"

_jinja_env = Environment(
    loader=FileSystemLoader(TEMPLATE_DIR),
    autoescape=select_autoescape(),
)


def render_config(**config) -> str:
    template = _jinja_env.get_template(TEMPLATE_MAIN)
    return template.render(**config)
