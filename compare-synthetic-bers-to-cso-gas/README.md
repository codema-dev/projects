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


# Compare Synthetic BERs to CSO Gas Consumption

- Download County & Dublin Postal District annual CSO Gas consumption data
- (closed-access) Download Synthetic Dublin Building Energy Ratings (BERs)
- Concatenate County Dublin gas consumption with Dublin Postal District consumption
- Amalgamate the BERs to postal districts
- Plot gas vs estimated heat 

## Setup

| ❗  Skip if running on Binder  |
|-------------------------------|

Via [conda](https://github.com/conda-forge/miniforge):

```{code-cell}
conda env create --name compare-bers-to-cso-gas --file environment.yml
conda activate compare-bers-to-cso-gas
```

## Run

```{code-cell}
!ploomber build
```
