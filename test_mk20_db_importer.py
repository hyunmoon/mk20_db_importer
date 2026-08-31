import csv
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import mk20_db_importer as importer


class ImporterSafetyTests(unittest.TestCase):
    PIECE_V1 = "baga6ea4seaq-test-piece-v1"
    PIECE_V2 = "baga6ea4seaq-test-piece-v2"

    def allocation(self, *, term_min=100, term_max=300):
        return importer.Allocation(
            allocation_id=42,
            client=1001,
            miner=2002,
            piece_cid=self.PIECE_V1,
            piece_size=2048,
            term_min=term_min,
            term_max=term_max,
            expiration=999999,
            raw={},
        )

    def candidates(self, *, duration=200, url=None, allocation=None):
        allocation = allocation or self.allocation()
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "provider.csv"
            with path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=importer.EXPECTED_CSV_COLUMNS)
                writer.writeheader()
                writer.writerow(
                    {
                        "data_cid": "bafy-test-data",
                        "piece_cid_v1": self.PIECE_V1,
                        "pcidv2": self.PIECE_V2,
                        "piece_size": "2048",
                        "car_size": "1024",
                        "car_url": url or f"https://example.test/{self.PIECE_V1}.car",
                    }
                )
            return importer.read_csv_candidates(
                path,
                "safe-batch",
                1_700_000_000_000,
                {self.PIECE_V1: [allocation]},
                1001,
                2002,
                2048,
                duration,
            )

    def test_duration_inside_allocation_range_is_valid(self):
        candidate = self.candidates(duration=200)[0]
        self.assertIsNone(candidate.file_reject_reason)

    def test_duration_below_term_min_is_rejected(self):
        candidate = self.candidates(duration=99)[0]
        self.assertIn("below allocation term_min", candidate.file_reject_reason)

    def test_duration_above_term_max_is_rejected(self):
        candidate = self.candidates(duration=301)[0]
        self.assertIn("above allocation term_max", candidate.file_reject_reason)

    def test_invalid_allocation_term_range_is_rejected(self):
        candidate = self.candidates(allocation=self.allocation(term_min=300, term_max=100))[0]
        self.assertIn("allocation term range is invalid", candidate.file_reject_reason)

    def test_non_http_car_url_is_rejected(self):
        candidate = self.candidates(url=f"file:///tmp/{self.PIECE_V1}.car")[0]
        self.assertIn("absolute http(s)", candidate.file_reject_reason)

    def test_car_url_credentials_are_rejected(self):
        candidate = self.candidates(
            url=f"https://user:secret@example.test/{self.PIECE_V1}.car"
        )[0]
        self.assertIn("embedded credentials", candidate.file_reject_reason)

    def test_safe_labels_cannot_escape_output_directory(self):
        for value in ("../escape", "/tmp/escape", "line\nbreak", "space name", ""):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    importer.safe_label(value, "batch name")
        self.assertEqual("batch_01.safe", importer.safe_label("batch_01.safe", "batch name"))

    def test_sql_ident_rejects_empty_unicode_and_truncated_names(self):
        for value in ("", "9starts_with_digit", "table-name", "éxample", "a" * 64):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    importer.sql_ident(value)
        self.assertEqual("audit_table_1", importer.sql_ident("audit_table_1"))

    def test_stage_table_must_stay_in_audit_namespace(self):
        self.assertEqual(
            "audit_mk20_import_stage",
            importer.validate_stage_table("audit_mk20_import_stage"),
        )
        for value in ("market_mk20_deal", "audit_other_stage"):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    importer.validate_stage_table(value)

    def test_psql_command_is_argv_not_shell_text(self):
        with mock.patch.object(importer.subprocess, "run") as run:
            importer.run_psql(
                "ysqlsh --host 'db host'",
                Path("/tmp/generated file.sql"),
            )
        run.assert_called_once_with(
            [
                "ysqlsh",
                "--host",
                "db host",
                "-v",
                "ON_ERROR_STOP=1",
                "-f",
                "/tmp/generated file.sql",
            ],
            check=True,
        )

    def test_insert_sql_is_serializable_and_cooperatively_locked(self):
        sql = importer.generate_insert_sql(
            "batch", "audit_mk20_import_stage", 10, "run"
        )
        self.assertIn("BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE", sql)
        self.assertIn("pg_advisory_xact_lock(1296778320)", sql)

    def test_generated_exception_messages_escape_quotes(self):
        sql = importer.generate_insert_sql(
            "batch", "audit_mk20_import_stage", 10, "run'quote"
        )
        self.assertIn("run''quote", sql)
        self.assertNotIn("run'quote batch", sql)

    def test_verify_sql_scopes_piece_duplicates_to_provider(self):
        sql = importer.generate_verify_sql(
            "batch", "audit_mk20_import_stage", "run"
        )
        self.assertIn("i.provider = d.ddo_v1 #>> '{ddo,provider}'", sql)
        self.assertIn(
            "GROUP BY d.ddo_v1 #>> '{ddo,provider}', d.piece_cid_v2",
            sql,
        )
        self.assertIn("IS DISTINCT FROM", sql)

    def test_rollback_refuses_empty_manifest_and_preserves_shared_refs(self):
        sql = importer.generate_rollback_sql("batch", "run")
        self.assertIn("run_id has no audited inserted rows", sql)
        self.assertIn("rr.ref_id = ANY(remaining.ref_ids)", sql)
        self.assertIn("NOT EXISTS", sql)


if __name__ == "__main__":
    unittest.main()
