"""
This module aims to define the Dataset class and the load_dataset api function.
"""
from typing import List
from typing import Optional
from typing import Tuple

import numpy as np
import pandas as pd

from biodatasets.utils import logger
from biodatasets.utils.google_bucket import list_blobs
from biodatasets.utils.google_bucket import pull_dataset

log = logger.get(__name__)

DATASETS_DIR_PATH = "datasets"


class Dataset:
    """
    Dataset class.
    """

    def __init__(self, name: str, force: bool = False):
        """
        Init the Dataset instance by fetching the dataset files.
        :param name: name of the dataset
        :param force: force fetching the dataset
        """
        self.name = name
        self.path = pull_dataset(name, force=force)

    def to_npy_arrays(
        self, inputs: List, targets: Optional[List]
    ) -> Tuple[Optional[np.ndarray], Optional[np.ndarray]]:
        """
        Load dataset inputs and targets into numpy arrays.

        :param inputs: list of input column names
        :param targets: list of target column names
        :return: arrays for inputs and targets
        """
        df = pd.read_csv(self.path / "dataset.csv")

        if not set(inputs).issubset(df.columns):
            log.error("Some inputs are not in the dataset.")
            return None, None
        inputs_array = df[inputs].values

        if targets is not None and not set(targets).issubset(df.columns):
            log.error("Some targets are not in the dataset.")
            return inputs_array, None
        targets_array = df[targets].values

        return inputs_array, targets_array

    def get_embeddings(self) -> np.ndarray:
        """Return a 2D numpy array with the pretrained embeddings for each sequence."""
        embeddings = np.load(self.path / "embeddings.npy")

        return embeddings


def list_datasets() -> List[str]:
    """List all the datasets in the bucket.

    :return: list of the datasets
    """
    blobs = list_blobs()
    dataset_names = list(set(map(lambda x: x.name.split("/")[0], blobs)))

    return dataset_names


def load_dataset(name: str, force: bool = False) -> Optional[Dataset]:
    """Load a bio-dataset.

    :param name: name of the dataset
    :param name: force fetching the dataset
    :return: a Dataset instance
    """
    if name not in list_datasets():
        log.error(f"Dataset {name} does not exist.")
        return None

    return Dataset(name, force=force)
