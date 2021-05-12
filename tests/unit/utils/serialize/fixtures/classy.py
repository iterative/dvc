class Prepare:
    split: float = 0.20
    seed = 20170428
    shuffle_dataset = True
    bag = {"mango", "apple", "orange"}


class Featurize:
    max_features = 3000
    ngrams = 2


class Train:
    seed = 123
    min_split: str
    optimizer: str = "Adam"

    def __init__(self):
        self.seed = 20170428
        self.n_est = 100
        self.min_split = 64
        self.data = {"key1": "value1", "key2": "value2"}
