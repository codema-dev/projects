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

# Compare Synthetic Residential Building Demand to Postcode Network Gas Consumption

+++

## What `pipeline.yaml` is doing:

![pipeline.png](pipeline.png)

## Run pipeline

On Binder:

```{code-cell} ipython3
%%writefile .env
AWS_ACCESS_KEY_ID="AKIA4DU27BZT6UZUHCYT"
AWS_SECRET_ACCESS_KEY="KXY6w49ws+s4XXqQbeo5AK3mZG/NN9mHkc/tHkQ1"
```

```{code-cell} ipython3
!ploomber build
```

OR on your Terminal:

```{code-cell} ipython3
ploomber build
```
