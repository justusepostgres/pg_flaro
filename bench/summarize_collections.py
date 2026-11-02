#!/usr/bin/env python3
"""Summarize paired collection benchmark trials without pooling percentiles."""

import argparse
import json
from collections import defaultdict
from statistics import median


def summarize(document):
    groups = defaultdict(list)
    for row in document["results"]:
        groups[
            row["cardinality"], row["pipeline"], row["operation"], row["server"]
        ].append(row)
    args = document["metadata"]["arguments"]
    repetitions = int(args["repeats"])
    lines = [
        "| Elements | Pipeline | Operation | Flaro ops/s | Redis ops/s | Ratio | Flaro p99 ms | Redis p99 ms |",
        "|---:|---:|---|---:|---:|---:|---:|---:|",
    ]
    for cardinality in map(int, args["cardinalities"].split(",")):
        for pipeline in map(int, args["pipelines"].split(",")):
            for operation in args["operations"].split(","):
                a, b = [
                    groups[cardinality, pipeline, operation, server]
                    for server in ("flaro", "redis")
                ]
                for trials in (a, b):
                    assert sorted(r["repeat"] for r in trials) == list(
                        range(repetitions)
                    ), "incomplete or duplicate trial group"
                throughput = [
                    median(r["ops_per_second"] for r in trials) for trials in (a, b)
                ]
                latency = [median(r["p99_ms"] for r in trials) for trials in (a, b)]
                lines.append(
                    f"| {cardinality:,} | {pipeline} | {operation} | {throughput[0]:,.0f} | {throughput[1]:,.0f} | {throughput[0] / throughput[1]:.2f}× | {latency[0]:.3f} | {latency[1]:.3f} |"
                )
    assert len(document["results"]) == len(groups) * repetitions
    return "\n".join(lines)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results")
    args = parser.parse_args()
    with open(args.results) as stream:
        print(summarize(json.load(stream)))
