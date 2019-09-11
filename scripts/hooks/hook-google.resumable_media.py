from PyInstaller.utils.hooks import copy_metadata

datas = copy_metadata("six")
datas += copy_metadata("requests")
datas += copy_metadata("google-resumable-media")
