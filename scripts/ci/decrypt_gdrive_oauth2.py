from subprocess import check_call
import os
import sys

from dvc.config import Config

OAUTH2_TOKEN_FILE_KEY = os.getenv("OAUTH2_TOKEN_FILE_KEY")
OAUTH2_TOKEN_FILE_IV = os.getenv("OAUTH2_TOKEN_FILE_IV")
if OAUTH2_TOKEN_FILE_KEY is None or OAUTH2_TOKEN_FILE_IV is None:
    print("{}:".format(sys.argv[0]))
    print("OAUTH2_TOKEN_FILE_KEY or OAUTH2_TOKEN_FILE_IV are not defined.")
    print("Skipping decrypt.")
    sys.exit(0)

src = os.path.join("scripts", "ci", "gdrive-oauth2")
dest = os.path.join(Config.get_global_config_dir(), "gdrive-oauth2")
if not os.path.exists(dest):
    os.makedirs(dest)

for enc_filename in os.listdir(src):
    filename, ext = os.path.splitext(enc_filename)
    if ext != ".enc":
        print("Skipping {}".format(enc_filename))
        continue
    print("Decrypting {}".format(enc_filename))
    cmd = [
        "openssl",
        "aes-256-cbc",
        "-d",
        "-K",
        OAUTH2_TOKEN_FILE_KEY,
        "-iv",
        OAUTH2_TOKEN_FILE_IV,
        "-in",
        os.path.join(src, enc_filename),
        "-out",
        os.path.join(dest, filename),
    ]
    check_call(cmd)
