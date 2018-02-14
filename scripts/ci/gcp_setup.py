import os

def get_env(name):
    ret = os.getenv(name, None)
    if not ret:
        print("Failed to obtain env var {}. Skipping GCP setup.".format(name))
        exit(0)
    return ret

path = get_env("GOOGLE_APPLICATION_CREDENTIALS")
contents = get_env("GCP_CREDS")

with open(path, 'w+') as fd:
    fd.write(contents)
