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


# Extract Dublin Electricity Lines & Substations

| :exclamation:  Requires ESB Networks CAD Network files |
|--------------------------------------------------------|
    
## What `pipeline.py` is doing:

![pipeline.png](pipeline.png)

## Install

| :exclamation:  Skip unless running this locally |
|-------------------------------------------------|

Via [conda](https://github.com/conda-forge/miniforge):

- Minimal

```{code-cell}
conda env create --file environment.yml --name extract-dublin-electricity-infrastructure
conda activate extract-dublin-electricity-infrastructure
```

## Run

Now run the pipeline:

```{code-cell}
!ploomber build
```
