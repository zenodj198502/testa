#!/usr/bin/env python3
"""
从 Perl 调度脚本中抽取 SQL，并生成数据血缘依赖（表级别 + 字段级别）。

用法示例：
  python lineage_tool.py --input ./jobs --output lineage.json --dot lineage.dot
  python lineage_tool.py --input ./jobs --field-output field_lineage.json --query-target-field ads.order_summary.amount
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Iterable


PERL_FILE_PATTERNS = ("*.pl", "*.pm", "*.perl")

# 常见 SQL 动作模式
INSERT_RE = re.compile(r"\binsert\s+into\s+([`\"\w.]+)", re.IGNORECASE)
INSERT_COLS_RE = re.compile(
    r"\binsert\s+into\s+([`\"\w.]+)\s*\((.*?)\)\s*select\s+(.*?)\s+from\s+",
    re.IGNORECASE | re.DOTALL,
)
CREATE_AS_RE = re.compile(r"\bcreate\s+table\s+([`\"\w.]+)\s+as\b", re.IGNORECASE)
CREATE_AS_SELECT_RE = re.compile(
    r"\bcreate\s+table\s+([`\"\w.]+)\s+as\s+select\s+(.*?)\s+from\s+",
    re.IGNORECASE | re.DOTALL,
)
UPDATE_RE = re.compile(r"\bupdate\s+([`\"\w.]+)", re.IGNORECASE)
DELETE_RE = re.compile(r"\bdelete\s+from\s+([`\"\w.]+)", re.IGNORECASE)

SOURCE_RE = re.compile(
    r"\b(?:from|join)\s+([`\"\w.]+)",
    re.IGNORECASE,
)

SQL_HINT_RE = re.compile(
    r"\b(select|insert|update|delete|create\s+table)\b",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class LineageEdge:
    source_table: str
    target_table: str
    sql_type: str
    file: str
    snippet: str


@dataclass(frozen=True)
class FieldLineageEdge:
    source_table: str
    source_field: str
    target_table: str
    target_field: str
    sql_type: str
    file: str
    snippet: str


def normalize_table_name(name: str) -> str:
    return name.strip().strip('`"').lower()


def normalize_field_name(name: str) -> str:
    return name.strip().strip('`"').lower()


def split_select_items(select_expr: str) -> list[str]:
    """简单按逗号拆分 select 字段，忽略括号内逗号。"""
    items: list[str] = []
    current: list[str] = []
    depth = 0
    for ch in select_expr:
        if ch == "(":
            depth += 1
        elif ch == ")" and depth > 0:
            depth -= 1

        if ch == "," and depth == 0:
            part = "".join(current).strip()
            if part:
                items.append(part)
            current = []
            continue
        current.append(ch)

    part = "".join(current).strip()
    if part:
        items.append(part)
    return items


def guess_source_field(expr: str) -> tuple[str, str]:
    """从 select 表达式中尽量识别 source_table/source_field。"""
    clean = " ".join(expr.split())

    # col as alias / col alias
    alias_split = re.split(r"\s+as\s+|\s+", clean, maxsplit=1, flags=re.IGNORECASE)
    left = alias_split[0]

    # a.col or db.tbl.col
    m = re.search(r"([`\"\w]+)\.([`\"\w]+)$", left)
    if m:
        return normalize_table_name(m.group(1)), normalize_field_name(m.group(2))

    # 只有列名，表未知
    m = re.search(r"([`\"\w]+)$", left)
    if m:
        return "unknown", normalize_field_name(m.group(1))

    return "unknown", "expr"


def find_perl_files(root: Path) -> Iterable[Path]:
    for pattern in PERL_FILE_PATTERNS:
        yield from root.rglob(pattern)


def extract_sql_blocks(perl_text: str) -> list[str]:
    candidates: list[str] = []

    for match in re.finditer(r"(['\"])(.*?)(?<!\\)\1", perl_text, re.DOTALL):
        text = match.group(2)
        if SQL_HINT_RE.search(text):
            candidates.append(text)

    for match in re.finditer(r"\bq[q]?\{(.*?)\}", perl_text, re.DOTALL | re.IGNORECASE):
        text = match.group(1)
        if SQL_HINT_RE.search(text):
            candidates.append(text)

    for match in re.finditer(r"<<\s*([A-Z_][A-Z0-9_]*)\s*;?\s*\n(.*?)\n\1\s*;?", perl_text, re.DOTALL):
        text = match.group(2)
        if SQL_HINT_RE.search(text):
            candidates.append(text)

    return candidates


def parse_lineage_from_sql(sql: str, file_path: str) -> list[LineageEdge]:
    sql_one_line = " ".join(sql.split())
    sources = [normalize_table_name(t) for t in SOURCE_RE.findall(sql_one_line)]
    sources = [s for s in sources if s]

    edges: list[LineageEdge] = []

    def build_edges(target_table: str, sql_type: str) -> None:
        target = normalize_table_name(target_table)
        if not target:
            return
        if sources:
            for src in sorted(set(sources)):
                if src != target:
                    edges.append(
                        LineageEdge(src, target, sql_type, file_path, sql_one_line[:240])
                    )
        else:
            edges.append(LineageEdge(target, target, sql_type, file_path, sql_one_line[:240]))

    for reg, sql_type in [
        (INSERT_RE, "insert"),
        (CREATE_AS_RE, "create_table_as"),
        (UPDATE_RE, "update"),
        (DELETE_RE, "delete"),
    ]:
        m = reg.search(sql_one_line)
        if m:
            build_edges(m.group(1), sql_type)

    return edges


def parse_field_lineage_from_sql(sql: str, file_path: str) -> list[FieldLineageEdge]:
    """字段级血缘（启发式）：
    1) INSERT INTO t(c1,c2) SELECT expr1,expr2 FROM ...
    2) CREATE TABLE t AS SELECT expr1,expr2 FROM ... （目标字段取 alias 或表达式末尾字段名）
    """
    sql_one_line = " ".join(sql.split())
    edges: list[FieldLineageEdge] = []

    m = INSERT_COLS_RE.search(sql_one_line)
    if m:
        target_table = normalize_table_name(m.group(1))
        target_cols = [normalize_field_name(x) for x in m.group(2).split(",") if x.strip()]
        select_items = split_select_items(m.group(3))
        for target_col, select_item in zip(target_cols, select_items):
            src_table, src_field = guess_source_field(select_item)
            edges.append(
                FieldLineageEdge(
                    source_table=src_table,
                    source_field=src_field,
                    target_table=target_table,
                    target_field=target_col,
                    sql_type="insert",
                    file=file_path,
                    snippet=sql_one_line[:240],
                )
            )

    m = CREATE_AS_SELECT_RE.search(sql_one_line)
    if m:
        target_table = normalize_table_name(m.group(1))
        select_items = split_select_items(m.group(2))
        for item in select_items:
            src_table, src_field = guess_source_field(item)
            # 目标字段：优先 alias，否则用 source_field
            alias_match = re.search(r"\bas\s+([`\"\w]+)$", item, re.IGNORECASE)
            target_field = normalize_field_name(alias_match.group(1)) if alias_match else src_field
            edges.append(
                FieldLineageEdge(
                    source_table=src_table,
                    source_field=src_field,
                    target_table=target_table,
                    target_field=target_field,
                    sql_type="create_table_as",
                    file=file_path,
                    snippet=sql_one_line[:240],
                )
            )

    return edges


def collect_lineage(input_dir: Path) -> list[LineageEdge]:
    all_edges: list[LineageEdge] = []
    for perl_file in find_perl_files(input_dir):
        try:
            text = perl_file.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue

        for sql_block in extract_sql_blocks(text):
            all_edges.extend(parse_lineage_from_sql(sql_block, str(perl_file)))

    unique = {(e.source_table, e.target_table, e.sql_type, e.file, e.snippet): e for e in all_edges}
    return sorted(unique.values(), key=lambda e: (e.target_table, e.source_table, e.file, e.sql_type))


def collect_field_lineage(input_dir: Path) -> list[FieldLineageEdge]:
    all_edges: list[FieldLineageEdge] = []
    for perl_file in find_perl_files(input_dir):
        try:
            text = perl_file.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        for sql_block in extract_sql_blocks(text):
            all_edges.extend(parse_field_lineage_from_sql(sql_block, str(perl_file)))

    unique = {
        (e.source_table, e.source_field, e.target_table, e.target_field, e.sql_type, e.file, e.snippet): e
        for e in all_edges
    }
    return sorted(unique.values(), key=lambda e: (e.target_table, e.target_field, e.source_table, e.source_field))


def query_field_lineage(edges: list[FieldLineageEdge], target_field_path: str) -> list[FieldLineageEdge]:
    # 入参格式：table.field
    if "." not in target_field_path:
        return []
    table, field = target_field_path.rsplit(".", 1)
    t = normalize_table_name(table)
    f = normalize_field_name(field)
    return [e for e in edges if e.target_table == t and e.target_field == f]


def to_dot(edges: list[LineageEdge]) -> str:
    lines = ["digraph data_lineage {", "  rankdir=LR;"]
    seen = set()
    for e in edges:
        key = (e.source_table, e.target_table)
        if key in seen:
            continue
        seen.add(key)
        lines.append(f'  "{e.source_table}" -> "{e.target_table}";')
    lines.append("}")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Perl SQL 血缘抽取工具（表级+字段级）")
    parser.add_argument("--input", required=True, help="Perl 脚本根目录")
    parser.add_argument("--output", default="lineage.json", help="表级血缘 JSON 输出文件")
    parser.add_argument("--dot", default="", help="可选 Graphviz DOT 输出文件")
    parser.add_argument("--field-output", default="", help="可选字段级血缘 JSON 输出文件")
    parser.add_argument(
        "--query-target-field",
        default="",
        help="查询某个目标字段的上游，格式 table.field，例如 ads.order_summary.amount",
    )
    args = parser.parse_args()

    input_dir = Path(args.input)
    if not input_dir.exists() or not input_dir.is_dir():
        raise SystemExit(f"输入目录不存在或不是目录: {input_dir}")

    table_edges = collect_lineage(input_dir)
    payload = {"edge_count": len(table_edges), "edges": [asdict(e) for e in table_edges]}
    Path(args.output).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"已输出 {len(table_edges)} 条表级血缘到 {args.output}")

    if args.dot:
        Path(args.dot).write_text(to_dot(table_edges), encoding="utf-8")
        print(f"已输出图结构到 {args.dot}")

    # 按需抽取字段级
    need_field = bool(args.field_output or args.query_target_field)
    if need_field:
        field_edges = collect_field_lineage(input_dir)

        if args.field_output:
            field_payload = {"edge_count": len(field_edges), "edges": [asdict(e) for e in field_edges]}
            Path(args.field_output).write_text(
                json.dumps(field_payload, ensure_ascii=False, indent=2), encoding="utf-8"
            )
            print(f"已输出 {len(field_edges)} 条字段级血缘到 {args.field_output}")

        if args.query_target_field:
            results = query_field_lineage(field_edges, args.query_target_field)
            print(f"字段查询: {args.query_target_field}，命中 {len(results)} 条")
            for e in results:
                print(f"  {e.source_table}.{e.source_field} -> {e.target_table}.{e.target_field} ({e.sql_type})")


if __name__ == "__main__":
    main()
