# Heritage Buildings

Extract all Dublin Heritage Buildings according to https://maps.archaeology.ie/historicenvironment/

## Setup

Via [conda](https://github.com/conda-forge/miniforge):

```bash
conda env create --name heritage --file environment.yml
conda activate heritage
```

Now run the pipeline:

```bash
ploomber build
```