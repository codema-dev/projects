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


# Road & Rail Emissions

Explore the `National Transport Authority of Ireland` Emission Model outputs in `Jupyter Notebooks`

## Caveats

To fully reproduce the pipeline the user must:

- Have access to:
    - `National Transport Authority of Ireland` model outputs 

## Setup

| ❗  Skip if running on Binder  |
|-------------------------------|

Via [conda](https://github.com/conda-forge/miniforge):

```{code-cell}
conda env create --file environment.yml
conda activate road-and-rail-emissions
```

## Run

```{code-cell}
jupyter noteook
```
