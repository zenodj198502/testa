# Perl SQL 数据溯源（表级血缘）原型

这是一个可直接运行的原型工具，用于：

1. 扫描批量调度中的 Perl 脚本（`.pl/.pm/.perl`）
2. 抽取脚本中出现的 SQL 片段（字符串、`q{}`、简单 heredoc）
3. 识别读写关系，输出表级依赖边（`source_table -> target_table`）

## 快速开始

```bash
python lineage_tool.py --input /path/to/perl/jobs --output lineage.json --dot lineage.dot
```

输出：

- `lineage.json`：详细血缘边（含来源文件、SQL 片段）
- `lineage.dot`：可用 Graphviz 渲染的图结构

## 当前支持的 SQL 规则

- 写入目标表识别：
  - `INSERT INTO target ...`
  - `CREATE TABLE target AS ...`
  - `UPDATE target ...`
  - `DELETE FROM target ...`
- 上游来源表识别：
  - `FROM source`
  - `JOIN source`

## 建议的下一步（生产化）

- 引入 SQL 解析器（按你实际数据库方言）替代正则，提高准确率
- 增加任务级元数据（job 名称、调度时间、owner）并关联血缘
- 做增量扫描和缓存，支持大规模脚本仓库
- 将结果落地到图数据库（如 Neo4j）支持影响分析与可视化
