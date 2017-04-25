from PyInstaller.utils.hooks import copy_metadata
datas = copy_metadata('google-cloud-storage')
datas.extend(copy_metadata('google-cloud-core'))
