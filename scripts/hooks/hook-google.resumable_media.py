from PyInstaller.utils.hooks import copy_metadata

datas = copy_metadata("google-resumable-media")
datas += copy_metadata("requests")
