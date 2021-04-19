# bio-datasets
Free collection of Bio datasets and pre-trained embeddings.

## Description
bio-datasets is a collaborative framework that allows the user to fetch publicly available sequence-based protein datasets.
For these datasets, pre-trained contextual embeddings are also available.


## Installation
It is recommended to work with conda environnements in order to manage the specific dependencies of the package.
```bash
  conda env create -f environment.yml
  conda activate datasets
  pip install -r requirements.txt
```

# How it works

```python
from biodatasets.dataset import list_datasets, load_dataset

list_datasets()

my_dataset = load_dataset('test')
X, y = my_dataset.to_npy_array(inputs=['peptide'], targets=['target'])

embeddings = my_dataset.get_embeddings()
```
