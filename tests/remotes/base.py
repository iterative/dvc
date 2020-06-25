from funcy import cached_property


class Base:
    @staticmethod
    def should_test():
        return True

    @staticmethod
    def get_url():
        raise NotImplementedError

    @cached_property
    def url(self):
        return self.get_url()

    @cached_property
    def config(self):
        return {"url": self.url}
