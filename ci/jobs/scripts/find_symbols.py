#!/usr/bin/env python3
import argparse
import io
import os
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path


class DiffToSymbols:
    def __init__(self, clickhouse_path: str, pr_number: int):
        if Path(clickhouse_path).is_dir():
            self.clickhouse_path = clickhouse_path + "/clickhouse"
        else:
            self.clickhouse_path = clickhouse_path
        # TODO: add support for non-API mode (from git)
        self.pr_number = pr_number
        assert self.pr_number > 0, "Works only for PRs"
        assert Path(
            self.clickhouse_path
        ).is_file(), f"clickhouse binary not found at {self.clickhouse_path}"

    @staticmethod
    def fetch(url: str, retries: int = 5, delay: float = 5.0) -> bytes:
        for attempt in range(retries):
            try:
                with urllib.request.urlopen(url) as resp:
                    return resp.read()
            except urllib.error.HTTPError as e:
                if e.code >= 500 and attempt < retries - 1:
                    print(
                        f"HTTP {e.code} fetching {url}, retrying in {delay}s (attempt {attempt + 1}/{retries})",
                        file=sys.stderr,
                    )
                    time.sleep(delay)
                    delay *= 2
                else:
                    raise

    @staticmethod
    def parse_diff_to_line_numbers(diff_bytes: bytes) -> list:
        """
        Return a list of (filename, line_number) tuples for added or removed lines
        in C/C++ source files found in the diff.
        """
        from unidiff import PatchSet

        patch = PatchSet(diff_bytes.decode("utf-8", errors="ignore"))
        result = []
        exts = (".cpp", ".cc", ".cxx", ".c", ".hpp", ".hh", ".hxx", ".h", ".ipp")
        for f in patch:
            if not f.path.endswith(exts):
                continue
            for hunk in f:
                for line in hunk:
                    if line.is_added:
                        # Added lines use target line number (new file)
                        result.append((f.path, line.target_line_no))
                    elif line.is_removed:
                        # Removed lines use source line number (old file)
                        result.append((f.path, line.source_line_no))
        return result

    @staticmethod
    def expand_line_numbers(line_numbers: list, radius: int) -> list:
        """Expand each (file, line) to include +/-radius nearby lines."""
        expanded = set()
        for filename, line_no in line_numbers:
            for offset in range(-radius, radius + 1):
                new_line = line_no + offset
                if new_line > 0:
                    expanded.add((filename, new_line))
        return sorted(expanded)

    def run_query(self, line_numbers: list, bidirectional: bool = False) -> list:
        """
        Execute a ClickHouse query with the provided (filename, line_number) tuples.

        Args:
            line_numbers: List of tuples (filename, line_number)
            bidirectional: If True, also find the nearest function *after* each
                changed line via a reverse ASOF JOIN (UNION ALL).

        Returns:
            List of (filename, line_number, address, linkage_name, symbol) tuples,
            deduplicated by (filename, line_number, symbol).
        """
        out = io.StringIO()
        out.write("filename,line\n")
        for filename, line_no in line_numbers:
            out.write("{},{}\n".format(filename, line_no))
        csv_payload = out.getvalue()

        dwarf_subquery = (
            "(SELECT decl_file, decl_line, linkage_name, ranges[1].1 AS address "
            "FROM file('{ch_path}', 'DWARF') "
            "WHERE (tag = 'subprogram') AND (notEmpty(linkage_name) OR address != 0) "
            "AND notEmpty(decl_file))"
        ).format(ch_path=self.clickhouse_path)

        select_expr = (
            "diff.filename, diff.line, binary.address, binary.linkage_name, "
            "if(empty(binary.linkage_name), "
            "demangle(addressToSymbol(binary.address)), "
            "demangle(binary.linkage_name)) AS symbol"
        )

        if bidirectional:
            fd, csv_path = tempfile.mkstemp(suffix=".csv", prefix="diff_lines_")
            try:
                with os.fdopen(fd, "w") as f:
                    f.write(csv_payload)

                diff_table = (
                    f"file('{csv_path}', 'CSVWithNames', "
                    f"'filename String, line UInt32')"
                )
                query = (
                    "SELECT {select_expr} "
                    "FROM {diff_table} AS diff "
                    "ASOF LEFT JOIN {dwarf} AS binary "
                    "ON basename(diff.filename) = basename(binary.decl_file) "
                    "AND diff.line >= binary.decl_line "
                    "UNION ALL "
                    "SELECT {select_expr} "
                    "FROM {diff_table} AS diff "
                    "ASOF LEFT JOIN {dwarf} AS binary "
                    "ON basename(diff.filename) = basename(binary.decl_file) "
                    "AND diff.line <= binary.decl_line "
                    "FORMAT TSV"
                ).format(
                    select_expr=select_expr,
                    diff_table=diff_table,
                    dwarf=dwarf_subquery,
                )

                proc = subprocess.run(
                    [self.clickhouse_path, "local", "--query", query],
                    text=True,
                    capture_output=True,
                    check=False,
                )
            finally:
                os.unlink(csv_path)
        else:
            query = (
                "SELECT {select_expr} "
                "FROM file('stdin', 'CSVWithNames', "
                "'filename String, line UInt32') AS diff "
                "ASOF LEFT JOIN {dwarf} AS binary "
                "ON basename(diff.filename) = basename(binary.decl_file) "
                "AND diff.line >= binary.decl_line "
                "FORMAT TSV"
            ).format(select_expr=select_expr, dwarf=dwarf_subquery)

            proc = subprocess.run(
                [self.clickhouse_path, "local", "--query", query],
                input=csv_payload,
                text=True,
                capture_output=True,
                check=False,
            )

        if proc.returncode != 0:
            print(proc.stderr, file=sys.stderr)
            raise SystemExit(proc.returncode)

        result = []
        seen = set()
        for line in proc.stdout.strip().split("\n"):
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) >= 5:
                filename, line_no, address, linkage_name, symbol = (
                    parts[0],
                    int(parts[1]),
                    parts[2],
                    parts[3],
                    parts[4],
                )
                key = (filename, line_no, symbol)
                if key not in seen:
                    seen.add(key)
                    result.append(
                        (filename, line_no, address, linkage_name, symbol)
                    )
            elif len(parts) >= 2:
                filename, line_no = parts[0], int(parts[1])
                key = (filename, line_no, None)
                if key not in seen:
                    seen.add(key)
                    result.append((filename, line_no, None, None, None))

        return result

    def get_file_with_line_numbers(self):
        diff_url = f"https://patch-diff.githubusercontent.com/raw/ClickHouse/ClickHouse/pull/{self.pr_number}.diff"
        diff_bytes = self.fetch(diff_url)
        return self.parse_diff_to_line_numbers(diff_bytes)

    def get_map_line_to_symbol(
        self, bidirectional=False, line_expansion_radius=0
    ):
        """
        Get symbols mapping for changed lines.

        Args:
            bidirectional: Also resolve the nearest function *after* each line
                via a reverse ASOF JOIN.
            line_expansion_radius: When > 0, expand each changed line to +/-N
                nearby lines before resolving symbols.

        Returns:
            List of (filename, line_number, address, linkage_name, symbol) tuples.
            Empty list if there are no changes in source code.
        """
        file_with_line_numbers = self.get_file_with_line_numbers()
        if not file_with_line_numbers:
            return []
        if line_expansion_radius > 0:
            file_with_line_numbers = self.expand_line_numbers(
                file_with_line_numbers, line_expansion_radius
            )
        return self.run_query(
            file_with_line_numbers, bidirectional=bidirectional
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="List changed symbols for a PR by parsing the diff and querying ClickHouse."
    )
    parser.add_argument("pr", help="Pull request number")
    parser.add_argument(
        "clickhouse_path",
        help='Path to the clickhouse binary (executed as "clickhouse local")',
    )
    args = parser.parse_args()
    dts = DiffToSymbols(args.clickhouse_path, int(args.pr))
    output = dts.get_map_line_to_symbol()
    symbols = set()
    print("\n")
    for file, line, address, linkage_name, symbol in output:
        if not address and not linkage_name:
            print(f"{file}:{line} ->\n     NOT RESOLVED\n")
        if symbol and symbol not in symbols:
            symbols.add(symbol)
            print(f"{file}:{line} ->\n     {symbol}\n")
