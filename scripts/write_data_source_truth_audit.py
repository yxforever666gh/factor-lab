#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from factor_lab.data_source_truth_audit import write_data_source_truth_audit


def main() -> None:
    parser = argparse.ArgumentParser(description="Write read-only data source truth audit artifacts.")
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--output-dir", default="artifacts/data_source_truth_audit")
    parser.add_argument("--knowledge-dir", default="knowledge")
    args = parser.parse_args()
    report = write_data_source_truth_audit(args.project_root, args.output_dir, args.knowledge_dir)
    print(json.dumps({
        "json": report.get("json_path"),
        "markdown": report.get("markdown_path"),
        "knowledge": report.get("knowledge_path"),
        "summary": report.get("summary"),
        "no_network": report.get("no_network"),
        "no_queue_write": report.get("no_queue_write"),
        "no_daemon_start": report.get("no_daemon_start"),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
