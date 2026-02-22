#!/usr/bin/env python3
"""
从 Perl 调度脚本中抽取 SQL，并生成数据血缘依赖（表级别）。

用法示例：
  python lineage_tool.py --input ./jobs --output lineage.json --dot lineage.dot
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
CREATE_AS_RE = re.compile(r"\bcreate\s+table\s+([`\"\w.]+)\s+as\b", re.IGNORECASE)
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


def normalize_table_name(name: str) -> str:
    """去除引号并统一为小写。"""
    return name.strip().strip('`"').lower()


def find_perl_files(root: Path) -> Iterable[Path]:
    for pattern in PERL_FILE_PATTERNS:
        yield from root.rglob(pattern)


def extract_sql_blocks(perl_text: str) -> list[str]:
    """
    使用启发式方式提取可能的 SQL 语句。

    支持：
    - 单/双引号字符串
    - q{} / qq{}
    - heredoc（简单匹配）
    """
    candidates: list[str] = []

    # 1) 常规字符串
    for match in re.finditer(r"(['\"])(.*?)(?<!\\)\1", perl_text, re.DOTALL):
        text = match.group(2)
        if SQL_HINT_RE.search(text):
            candidates.append(text)

    # 2) q{} / qq{}
    for match in re.finditer(r"\bq[q]?\{(.*?)\}", perl_text, re.DOTALL | re.IGNORECASE):
        text = match.group(1)
        if SQL_HINT_RE.search(text):
            candidates.append(text)

    # 3) heredoc: <<SQL ... SQL
    for match in re.finditer(r"<<\s*([A-Z_][A-Z0-9_]*)\s*\n(.*?)\n\1", perl_text, re.DOTALL):
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
                        LineageEdge(
                            source_table=src,
                            target_table=target,
                            sql_type=sql_type,
                            file=file_path,
                            snippet=sql_one_line[:240],
                        )
                    )
        else:
            # 没有 from/join 时，记录自环，表示无法定位上游但存在写入
            edges.append(
                LineageEdge(
                    source_table=target,
                    target_table=target,
                    sql_type=sql_type,
                    file=file_path,
                    snippet=sql_one_line[:240],
                )
            )

    m = INSERT_RE.search(sql_one_line)
    if m:
        build_edges(m.group(1), "insert")

    m = CREATE_AS_RE.search(sql_one_line)
    if m:
        build_edges(m.group(1), "create_table_as")

    m = UPDATE_RE.search(sql_one_line)
    if m:
        build_edges(m.group(1), "update")

    m = DELETE_RE.search(sql_one_line)
    if m:
        build_edges(m.group(1), "delete")

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

    # 去重（按完整字段）
    unique = {(e.source_table, e.target_table, e.sql_type, e.file, e.snippet): e for e in all_edges}
    return sorted(unique.values(), key=lambda e: (e.target_table, e.source_table, e.file, e.sql_type))


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
    parser = argparse.ArgumentParser(description="Perl SQL 表级血缘抽取工具")
    parser.add_argument("--input", required=True, help="Perl 脚本根目录")
    parser.add_argument("--output", default="lineage.json", help="JSON 输出文件")
    parser.add_argument("--dot", default="", help="可选 Graphviz DOT 输出文件")
    args = parser.parse_args()

    input_dir = Path(args.input)
    if not input_dir.exists() or not input_dir.is_dir():
        raise SystemExit(f"输入目录不存在或不是目录: {input_dir}")

    edges = collect_lineage(input_dir)
    payload = {
        "edge_count": len(edges),
        "edges": [asdict(e) for e in edges],
    }

    Path(args.output).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"已输出 {len(edges)} 条血缘到 {args.output}")

    if args.dot:
        Path(args.dot).write_text(to_dot(edges), encoding="utf-8")
        print(f"已输出图结构到 {args.dot}")


if __name__ == "__main__":
    main()
