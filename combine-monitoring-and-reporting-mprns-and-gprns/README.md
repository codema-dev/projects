# Monitoring & Reporting

## What `pipeline.py` is doing:

- Load:
    - Monitoring & Reporting Data
- Link MPRNs to GPRNs

## Caveat

The M&R data is publicly available, however, the user still needs to [create their own s3 credentials](https://aws.amazon.com/s3/) to fully reproduce the pipeline this pipeline (*i.e. they need an AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY*)

## Setup

Via [conda](https://github.com/conda-forge/miniforge):

```bash
conda env create --file environment.yml
conda activate hdd
```

Now run the pipeline:

```bash
python pipeline.py
```