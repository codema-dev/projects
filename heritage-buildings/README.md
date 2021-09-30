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


# Heritage Buildings

Extract all Dublin Heritage Buildings according to https://maps.archaeology.ie/historicenvironment/

## Setup

| ❗  Skip if running on Binder  |
|-------------------------------|


Via [conda](https://github.com/conda-forge/miniforge):

```{code-cell}
conda env create --name heritage --file environment.yml
conda activate heritage
```

## Run

```{code-cell}
!ploomber build
```
