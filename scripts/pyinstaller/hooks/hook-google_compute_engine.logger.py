from PyInstaller.utils.hooks import copy_metadata  # pylint:disable=import-error

datas = copy_metadata("google-compute-engine")
hiddenimports = ["google_cloud_engine"]
