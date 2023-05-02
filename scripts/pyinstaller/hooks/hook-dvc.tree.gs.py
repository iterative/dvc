from PyInstaller.utils.hooks import copy_metadata  # pylint:disable=import-error

datas = copy_metadata("google-cloud-storage")

# NOTE: including "requests" metadata because of google-resumable-media
# bug. See [1] for more info.
#
# [1] https://github.com/googleapis/google-resumable-media-python/issues/59
datas += copy_metadata("requests")
