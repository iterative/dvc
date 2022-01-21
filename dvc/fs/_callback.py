import fsspec

from dvc.progress import Tqdm


class FsspecCallback(fsspec.Callback):
    def __init__(self, progress_bar):
        self.progress_bar = progress_bar
        super().__init__()

    def set_size(self, size):
        if size is not None:
            self.progress_bar.total = size
            self.progress_bar.refresh()
            super().set_size(size)

    def relative_update(self, inc=1):
        self.progress_bar.update(inc)
        super().relative_update(inc)

    def absolute_update(self, value):
        self.progress_bar.update_to(value)
        super().absolute_update(value)

    @staticmethod
    def wrap_fn(cb, fn):
        def wrapped(*args, **kwargs):
            res = fn(*args, **kwargs)
            cb.relative_update()
            return res

        return wrapped


def tdqm_or_callback_wrapped(
    fobj, method, total, callback=None, **pbar_kwargs
):
    if callback:
        from funcy import nullcontext
        from tqdm.utils import CallbackIOWrapper

        callback.set_size(total)
        wrapper = CallbackIOWrapper(callback.relative_update, fobj, method)
        return nullcontext(wrapper)

    return Tqdm.wrapattr(fobj, method, total=total, bytes=True, **pbar_kwargs)


DEFAULT_CALLBACK = fsspec.callbacks.NoOpCallback()
