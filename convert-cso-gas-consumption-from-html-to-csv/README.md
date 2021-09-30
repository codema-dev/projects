---
jupytext:
  cell_metadata_filter: -all
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.13.0
kernelspec:
  display_name: Python 3 (ipykernel)
  language: python
  name: python3
---

# Convert CSO Gas Consumption from HTML to CSV

- Download the annual CSO Gas consumption data as a HTML file
- Extract, clean and convert the tables to CSV

## Setup

| ❗  Skip if running on Binder  |
|-------------------------------|

Via [conda](https://github.com/conda-forge/miniforge):

```{code-cell} ipython3
conda env create --name convert-cso-gas-from-html-to-csv --file environment.yml
conda activate convert-cso-gas-from-html-to-csv
```

## Run

```{code-cell} ipython3
!ploomber build
```
