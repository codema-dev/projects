from collections import defaultdict
import json
from typing import Any
from zipfile import ZipFile

import pandas as pd


def convert_benchmark_uses_to_json(upstream: Any, product: Any) -> None:
    uses_grouped_by_category = defaultdict()
    with ZipFile(upstream["download_benchmark_uses"]) as zf:
        for filename in zf.namelist():
            name = filename.split("/")[-1].replace(".txt", "")
            with zf.open(filename, "r") as f:
                uses_grouped_by_category[name] = [
                    line.rstrip().decode("utf-8") for line in f
                ]
    benchmark_uses = {i: k for k, v in uses_grouped_by_category.items() for i in v}
    with open(product, "w") as f:
        json.dump(benchmark_uses, f)


def link_valuation_office_to_benchmarks(upstream: Any, product: Any) -> None:
    valuation_office = pd.read_csv(upstream["download_buildings"])
    benchmarks = pd.read_csv(upstream["download_benchmarks"])
    with open(upstream["convert_benchmark_uses_to_json"], "r") as f:
        benchmark_uses = json.load(f)

    valuation_office["Benchmark"] = valuation_office["Use1"].map(benchmark_uses)
    valuation_office_with_benchmarks = valuation_office.merge(
        benchmarks, on="Benchmark", how="left"
    )

    valuation_office_with_benchmarks.to_csv(product)
