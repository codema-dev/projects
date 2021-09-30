# Welcome to `codema-dev` projects!

Download, wrangle & explore all Irish energy datasets used by the `codema-dev` team

| ⚠️ Some projects use closed-access datasets for which you will need permission from the `codema-dev` team to use! |
|--------------------------------------------------------------------------------------|

## Setup

Run the projects in your browser by clicking on the following buttons:

| Button | ⚠️ Caveats |
| --- | --- |
| [![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/codema-dev/projects/main) | Setting up this workspace can take a few minutes! |
| [![Gitpod ready-to-code](https://img.shields.io/badge/Gitpod-ready--to--code-908a85?logo=gitpod)](https://gitpod.io/#https://github.com/codema-dev/projects) | Requires manual installation of of project-by-project dependencies |

## Why?

In previous years all data wrangling was performed solely using `Microsoft Excel`.   Although this is useful for small datasets, it soon becomes a burden when working with multiple, large datasets.

For example, when generating the previous residential energy estimates it was necessary to create up to 16 separate workbooks for each local authority each containing as many as 15 sheets, as the datasets were too large to fit into a single workbook.  Although each workbook performed the same logic to clean and merge datasets, changing this logic meant changing all of the separate workbooks one at a time.

Moving to open-source scripting tools enabled using logic written down in scripts (or text files) to wrangle and merge data files, thus separating data from the logic operating on it.  This means that if any dataset is updated, re-generating outputs is as simple as running a few scripts.  Furthermore these scripts can be shared without sharing the underlying datasets.  

## Running locally

- Install [Anaconda](https://docs.anaconda.com/anaconda/install/index.html) or [miniconda](https://github.com/conda-forge/miniforge)
- Follow the setup guide for each project!