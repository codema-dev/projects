---
jupytext:
  cell_metadata_filter: -all
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.12.0
kernelspec:
  display_name: Python 3 (ipykernel)
  language: python
  name: python3
---


# Ireland Small Area Statistics

1. Melt Census 2016 Ireland residential dwellings to individual building level
2. Link to postcodes - *for merging with BERs*

## Setup

| ❗  Skip if running on Binder  |
|-------------------------------|

Via [conda](https://github.com/conda-forge/miniforge):
```
conda env create --file environment.yml
conda activate ireland-small-area-statistics
```

## Run

```
python pipeline.py
```