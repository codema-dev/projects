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

<details>
<summary>⚠️ Before running the pipeline you must first fetch the ESB Networks CAD Network files</summary>

- Create a new folder called `data` and drag & drop `ESBdata_20210107.zip` to a new folder within it called `raw`
</details>

## What `pipeline.py` is doing:

![pipeline.png](pipeline.png)

## Run pipeline

On Binder:

```{code-cell} ipython3
!ploomber build
```

OR on your Terminal:

```{code-cell} ipython3
ploomber build
```
