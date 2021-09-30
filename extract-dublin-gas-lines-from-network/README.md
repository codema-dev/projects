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


# Extract Dublin Gas Lines

| :exclamation:  Requires GNI Network files |
|-------------------------------------------|

## What `pipeline.py` is doing:

![pipeline.png](pipeline.png)

## Setup

| ❗  Skip if running on Binder  |
|-------------------------------|

Via [conda](https://github.com/conda-forge/miniforge):

```{code-cell}
conda env create --file environment.yml --name extract-dublin-gas-lines-from-network
conda activate extract-dublin-gas-lines-from-network
```

## Run 

Now run the pipeline:

```{code-cell}
!ploomber build
```
