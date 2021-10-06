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

# Aggregate Urban Atlas footprints to Small Areas

> ⚠️ You must manually download the Dublin Urban Atlas data from https://land.copernicus.eu/local/urban-atlas and save it to a new folder called data/raw

+++

## What `pipeline.yaml` is doing:

![pipeline.png](pipeline.png)

+++

## Run pipeline

On Binder:
```{code-cell} bash
!ploomber build
```

OR on your Terminal:
```{code-cell} bash
ploomber build
```
