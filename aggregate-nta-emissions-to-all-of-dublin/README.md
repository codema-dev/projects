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

<details>
<summary>⚠️ Before running the pipeline you must first fetch the National Transport Authority data</summary>

> - Create a new folder called `data` and drag & drop `Dublin_Rail_Links.zip` and `NTA_grid_boundaries.zip` to a new folder within it called `raw`
</details>

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
