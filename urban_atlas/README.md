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


# Urban Atlas

Calculate the area of each urban atlas polygon type (i.e. Discontinuous Dense Urban Fabric, Continuous Urban Fabric etc) in each CSO Small Area boundary

> **Caveat**: You must manually download the Urban Atlas data from https://land.copernicus.eu/local/urban-atlas and save it to a new folder called data/raw

## Setup

| ❗  Skip if running on Binder  |
|-------------------------------|

Via [conda](https://github.com/conda-forge/miniforge):

```{code-cell}
conda env create --name urban-fabric --file environment.yml
conda activate urban-fabric
```

## Run

```{code-cell}
!ploomber build
```
