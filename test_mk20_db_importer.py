import csv
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import mk20_db_importer as importer


# FRC-0069 / go-fil-commcid reference vectors.
# https://github.com/filecoin-project/go-fil-commcid/blob/master/commcid_test.go
V1_508 = "baga6ea4seaqes3nobte6ezpp4wqan2age2s5yxcatzotcvobhgcmv5wi2xh5mbi"
V2_508 = "bafkzcibcaaces3nobte6ezpp4wqan2age2s5yxcatzotcvobhgcmv5wi2xh5mbi"
V1_EMPTY_32_GIB = "baga6ea4seaqao7s73y24kcutaosvacpdjgfe5pw76ooefnyqw4ynr3d2y6x2mpq"
V2_EMPTY_32_GIB = "bafkzcibcaapao7s73y24kcutaosvacpdjgfe5pw76ooefnyqw4ynr3d2y6x2mpq"
V1_1016 = "baga6ea4seaqn42av3szurbbscwuu3zjssvfwbpsvbjf6y3tukvlgl2nf5rha6pa"
V2_1016 = "bafkzcibcaac542av3szurbbscwuu3zjssvfwbpsvbjf6y3tukvlgl2nf5rha6pa"


class ImporterSafetyTests(unittest.TestCase):
    CLIENT_ID = 1001
    PROVIDER = "f03199233"
    PROVIDER_ID = 3199233

    def allocation(
        self,
        *,
        piece_cid=V1_508,
        piece_size=512,
        allocation_id=42,
        term_min=100,
        term_max=300,
    ):
        return importer.Allocation(
            allocation_id=allocation_id,
            client=self.CLIENT_ID,
            miner=self.PROVIDER_ID,
            piece_cid=piece_cid,
            piece_size=piece_size,
            term_min=term_min,
            term_max=term_max,
            expiration=999999,
            raw={},
        )

    def valid_row(
        self,
        *,
        piece_cid_v1=V1_508,
        piece_cid_v2=V2_508,
        piece_size=512,
        car_size=508,
        car_url=None,
    ):
        return {
            "data_cid": "bafybeigdyr-reference-data",
            "piece_cid_v1": piece_cid_v1,
            "pcidv2": piece_cid_v2,
            "piece_size": str(piece_size),
            "car_size": str(car_size),
            "car_url": car_url or f"https://example.test/{piece_cid_v1}.car",
        }

    def candidates(
        self,
        *,
        rows=None,
        allocations_by_piece=None,
        piece_size_expected=512,
        duration=200,
    ):
        rows = rows or [self.valid_row()]
        allocations_by_piece = allocations_by_piece or {
            V1_508: [self.allocation()]
        }
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "provider.csv"
            with path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(
                    handle, fieldnames=importer.EXPECTED_CSV_COLUMNS
                )
                writer.writeheader()
                writer.writerows(rows)
            return importer.read_csv_candidates(
                path,
                "safe batch",
                1_700_000_000_000,
                allocations_by_piece,
                self.CLIENT_ID,
                self.PROVIDER_ID,
                piece_size_expected,
                duration,
            )

    def allocation_record(self, *, allocation_id=42):
        return {
            "allocationid": allocation_id,
            "client": self.CLIENT_ID,
            "miner": self.PROVIDER_ID,
            "piececid": V1_508,
            "piecesize": 512,
            "termmin": 100,
            "termmax": 300,
            "expiration": 999999,
        }

    def parse_allocations(self, document):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "allocations.json"
            path.write_text(json.dumps(document), encoding="utf-8")
            return importer.read_allocations(path)

    def cli_args(self, *extra):
        return [
            "mk20_db_importer.py",
            "--allocations",
            "allocations.json",
            "--csv",
            "provider.csv",
            "--deal-client",
            "f1client",
            "--client-id",
            str(self.CLIENT_ID),
            "--provider",
            self.PROVIDER,
            "--provider-id",
            str(self.PROVIDER_ID),
            "--piece-size",
            "512",
            "--duration",
            "200",
            *extra,
        ]

    def test_reference_vector_508_raw_512_padded(self):
        info = importer.piece_cid_v2_info(V2_508)
        self.assertEqual(V1_508, info.piece_cid_v1)
        self.assertEqual(508, info.payload_size)
        self.assertEqual(512, info.padded_size)
        self.assertEqual(4, info.tree_height)

    def test_reference_vector_empty_32_gib(self):
        info = importer.piece_cid_v2_info(V2_EMPTY_32_GIB)
        self.assertEqual(V1_EMPTY_32_GIB, info.piece_cid_v1)
        self.assertEqual((32 << 30) * 127 // 128, info.payload_size)
        self.assertEqual(32 << 30, info.padded_size)
        self.assertEqual(30, info.tree_height)

    def test_reference_vector_1016_raw_1024_padded(self):
        info = importer.piece_cid_v2_info(V2_1016)
        self.assertEqual(V1_1016, info.piece_cid_v1)
        self.assertEqual(1016, info.payload_size)
        self.assertEqual(1024, info.padded_size)
        self.assertEqual(5, info.tree_height)

    def test_normal_candidate_uses_valid_reference_piece_cids(self):
        candidate = self.candidates()[0]
        self.assertIsNone(candidate.file_reject_reason)

    def test_valid_piece_cid_v2_with_mismatched_v1_is_rejected(self):
        row = self.valid_row(piece_cid_v1=V1_1016)
        allocation = self.allocation(piece_cid=V1_1016)
        candidate = self.candidates(
            rows=[row],
            allocations_by_piece={V1_1016: [allocation]},
        )[0]
        self.assertIn(
            "pcidv2 commitment does not match piece_cid_v1",
            candidate.file_reject_reason,
        )

    def test_valid_piece_cid_v2_with_mismatched_padded_size_is_rejected(self):
        row = self.valid_row(piece_size=1024)
        allocation = self.allocation(piece_size=1024)
        candidate = self.candidates(
            rows=[row],
            allocations_by_piece={V1_508: [allocation]},
            piece_size_expected=1024,
        )[0]
        self.assertIn(
            "pcidv2 padded size does not match csv piece_size: 512 != 1024",
            candidate.file_reject_reason,
        )

    def test_malformed_piece_cid_v2_is_rejected(self):
        row = self.valid_row(piece_cid_v2="not-a-piece-cid")
        candidate = self.candidates(rows=[row])[0]
        self.assertIn("invalid pcidv2", candidate.file_reject_reason)

    def test_provider_id_address_pair_is_accepted(self):
        self.assertEqual(
            self.PROVIDER_ID,
            importer.validate_provider_id_address(
                self.PROVIDER, self.PROVIDER_ID
            ),
        )

    def test_mismatched_provider_id_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "not --provider-id"):
            importer.validate_provider_id_address(
                self.PROVIDER, self.PROVIDER_ID + 1
            )

    def test_non_id_provider_address_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "Filecoin ID address"):
            importer.validate_provider_id_address(
                "f1abcdefghijklmnopqrstuvwxyz", self.PROVIDER_ID
            )

    def test_duplicate_allocation_id_in_csv_batch_is_rejected(self):
        candidates = self.candidates(rows=[self.valid_row(), self.valid_row()])
        self.assertEqual(2, len(candidates))
        for candidate in candidates:
            self.assertIn(
                "duplicate allocation_id in csv batch: 42",
                candidate.file_reject_reason,
            )

    def test_allocation_json_accepts_nested_list(self):
        allocations = self.parse_allocations(
            {"allocations": [self.allocation_record()]}
        )
        self.assertEqual([42], [allocation.allocation_id for allocation in allocations])

    def test_allocation_json_accepts_nested_object(self):
        record = self.allocation_record()
        record.pop("allocationid")
        allocations = self.parse_allocations(
            {"allocations": {"42": record}}
        )
        self.assertEqual([42], [allocation.allocation_id for allocation in allocations])

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
        allocation = self.allocation(term_min=300, term_max=100)
        candidate = self.candidates(
            allocations_by_piece={V1_508: [allocation]}
        )[0]
        self.assertIn("allocation term range is invalid", candidate.file_reject_reason)

    def test_non_http_car_url_is_rejected(self):
        row = self.valid_row(car_url=f"file:///tmp/{V1_508}.car")
        candidate = self.candidates(rows=[row])[0]
        self.assertIn("absolute http(s)", candidate.file_reject_reason)

    def test_http_url_without_hostname_is_rejected(self):
        row = self.valid_row(car_url=f"https:///{V1_508}.car")
        candidate = self.candidates(rows=[row])[0]
        self.assertIn("absolute http(s)", candidate.file_reject_reason)

    def test_car_url_credentials_are_rejected(self):
        row = self.valid_row(
            car_url=f"https://user:secret@example.test/{V1_508}.car"
        )
        candidate = self.candidates(rows=[row])[0]
        self.assertIn("embedded credentials", candidate.file_reject_reason)

    def test_nonpositive_car_size_is_rejected(self):
        for car_size in (0, -1):
            with self.subTest(car_size=car_size):
                candidate = self.candidates(
                    rows=[self.valid_row(car_size=car_size)]
                )[0]
                self.assertIn("car_size must be positive", candidate.file_reject_reason)

    def test_ulid_timestamp_outside_48_bits_is_rejected(self):
        for timestamp in (-1, 1 << 48):
            with self.subTest(timestamp=timestamp):
                with self.assertRaisesRegex(ValueError, "48 bits"):
                    importer.ulid_from_time_and_key(timestamp, "key")
        self.assertEqual(
            26,
            len(importer.ulid_from_time_and_key((1 << 48) - 1, "key")),
        )

    def test_limit_zero_is_rejected_before_file_access(self):
        with mock.patch.object(
            importer.sys, "argv", self.cli_args("--limit", "0")
        ):
            with self.assertRaisesRegex(SystemExit, "--limit must be a positive"):
                importer.main()

    def test_no_db_execute_is_rejected_before_file_access(self):
        with mock.patch.object(
            importer.sys,
            "argv",
            self.cli_args(
                "--no-db",
                "--execute",
                "--ack-db-direct",
                "--limit",
                "1",
            ),
        ):
            with self.assertRaisesRegex(
                SystemExit, "--no-db cannot be combined with --execute"
            ):
                importer.main()

    def test_label_policy_rejects_only_path_and_line_hazards(self):
        invalid = ("", "../escape", "dir/name", "dir\\name", "line\nbreak",
                   "line\rbreak", "nul\x00byte", "a" * 129)
        for value in invalid:
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    importer.safe_label(value, "batch name")
        for value in ("batch_01.safe", "batch with spaces", "operator's batch"):
            with self.subTest(value=value):
                self.assertEqual(value, importer.safe_label(value, "batch name"))

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

    def test_psql_command_is_argv_and_surfaces_failures(self):
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

    def test_insert_sql_lock_precedes_conflict_check(self):
        sql = importer.generate_insert_sql(
            "batch", "audit_mk20_import_stage", 10, "run"
        )
        self.assertIn("BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE", sql)
        lock_position = sql.index("pg_advisory_xact_lock(1296778320)")
        conflict_check_position = sql.index("CREATE TEMP TABLE picked")
        self.assertLess(lock_position, conflict_check_position)
        self.assertEqual(1, sql.count("pg_advisory_xact_lock(1296778320)"))

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

    def test_rollback_sql_checks_manifest_piece_cid_v1(self):
        sql = importer.generate_rollback_sql("batch", "run")
        self.assertIn(
            "direct_piece_activation_manifest #>> '{CID,/}' = i.piece_cid_v1",
            sql,
        )

    def test_rollback_refuses_empty_manifest_and_preserves_shared_refs(self):
        sql = importer.generate_rollback_sql("batch", "run")
        self.assertIn("run_id has no audited inserted rows", sql)
        self.assertIn("rr.ref_id = ANY(remaining.ref_ids)", sql)
        self.assertIn("NOT EXISTS", sql)

    def test_observe_sql_checks_manifest_piece_cid_v1(self):
        sql = importer.generate_observe_sql("batch", "run")
        self.assertIn(
            "direct_piece_activation_manifest #>> '{CID,/}' = i.piece_cid_v1",
            sql,
        )

    def test_observe_sql_covers_each_pipeline_stage(self):
        sql = importer.generate_observe_sql("batch", "run")
        for section in (
            "waiting_joined",
            "download_pipeline",
            "mk20_pipeline",
            "sdr_initial_pieces",
        ):
            with self.subTest(section=section):
                self.assertIn(f"SELECT '{section}' AS section", sql)


if __name__ == "__main__":
    unittest.main()
