# Perl SQL 数据溯源原型（表级 + 字段级）

用于批量调度场景中从 Perl 脚本提取 SQL，并生成血缘关系：

1. **表级血缘**：`source_table -> target_table`
2. **字段级血缘**：`source_table.source_field -> target_table.target_field`

## 快速开始

```bash
python lineage_tool.py --input /path/to/perl/jobs --output lineage.json --dot lineage.dot
```

字段级输出与查询：

```bash
python lineage_tool.py \
  --input /path/to/perl/jobs \
  --output lineage.json \
  --field-output field_lineage.json \
  --query-target-field ads.order_summary.amount
```

## 当前能力

### SQL 抽取
- 单/双引号字符串
- `q{}` / `qq{}`
- heredoc（支持 `<<SQL; ... SQL`）

### 表级血缘规则
- 目标表：
  - `INSERT INTO target ...`
  - `CREATE TABLE target AS ...`
  - `UPDATE target ...`
  - `DELETE FROM target ...`
- 来源表：
  - `FROM source`
  - `JOIN source`

### 字段级血缘规则（启发式）
- `INSERT INTO t(c1,c2,...) SELECT e1,e2,... FROM ...`
- `CREATE TABLE t AS SELECT e1,e2,... FROM ...`
- 字段映射按位置对齐，目标字段优先取显式列名/alias

## 生产化建议
- 引入 SQL AST 解析器（按方言）替代正则
- 解析 CTE / 子查询 / union / 多层表达式
- 增加任务元数据（job、owner、调度批次）并关联血缘
- 将血缘写入图数据库支持影响分析与可视化
