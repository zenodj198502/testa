import json
import subprocess
import tempfile
from pathlib import Path
import unittest


class TestLineageToolCLI(unittest.TestCase):
    def test_extract_table_and_field_lineage_and_query(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            jobs = root / "jobs"
            jobs.mkdir(parents=True, exist_ok=True)

            (jobs / "job1.pl").write_text(
                'my $sql = "insert into dwd.orders(order_id,amount) select src.id, src.pay_amt from ods.orders_src src";\n',
                encoding="utf-8",
            )
            (jobs / "job2.pl").write_text(
                'my $sql = q{create table ads.order_summary as select o.amount as amount, o.order_id as order_id from dwd.orders o};\n',
                encoding="utf-8",
            )
            (jobs / "job3.pl").write_text(
                """my $sql = <<SQL;\nupdate dwd.orders set amount = 0 where id in (select id from ods.bad_orders);\nSQL\n""",
                encoding="utf-8",
            )

            out_json = root / "lineage.json"
            out_dot = root / "lineage.dot"
            out_field_json = root / "field_lineage.json"

            cmd = [
                "python",
                "lineage_tool.py",
                "--input",
                str(jobs),
                "--output",
                str(out_json),
                "--dot",
                str(out_dot),
                "--field-output",
                str(out_field_json),
                "--query-target-field",
                "ads.order_summary.amount",
            ]
            result = subprocess.run(
                cmd,
                cwd="/workspace/testa",
                capture_output=True,
                text=True,
                check=True,
            )

            table_payload = json.loads(out_json.read_text(encoding="utf-8"))
            self.assertGreaterEqual(table_payload["edge_count"], 3)

            field_payload = json.loads(out_field_json.read_text(encoding="utf-8"))
            edge_pairs = {
                (e["source_table"], e["source_field"], e["target_table"], e["target_field"]) for e in field_payload["edges"]
            }

            self.assertIn(("src", "id", "dwd.orders", "order_id"), edge_pairs)
            self.assertIn(("src", "pay_amt", "dwd.orders", "amount"), edge_pairs)
            self.assertIn(("o", "amount", "ads.order_summary", "amount"), edge_pairs)

            self.assertTrue(out_dot.exists())
            self.assertIn("字段查询: ads.order_summary.amount，命中", result.stdout)


if __name__ == "__main__":
    unittest.main()
