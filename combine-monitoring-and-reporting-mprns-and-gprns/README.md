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


# Monitoring & Reporting

## What `pipeline.py` is doing:

- Load:
    - Monitoring & Reporting Data
- Link MPRNs to GPRNs

## Caveat

The M&R data is publicly available, however, the user still needs to [create their own s3 credentials](https://aws.amazon.com/s3/) to fully reproduce the pipeline this pipeline (*i.e. they need an AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY*)

## Setup

| ❗  Skip if running on Binder  |
|-------------------------------|

Via [conda](https://github.com/conda-forge/miniforge):

```{code-cell}
conda env create --file environment.yml
conda activate hdd
```

## Run

```{code-cell}
python pipeline.py
```
