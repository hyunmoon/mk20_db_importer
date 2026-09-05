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
V1_508_ALT = "baga6ea4seaqes3nobte6ezpp4wqan2age2s5yxcatzotcvobhgcmv5wi2xh5mba"
V2_508_ALT = "bafkzcibcaaces3nobte6ezpp4wqan2age2s5yxcatzotcvobhgcmv5wi2xh5mba"
V1_EMPTY_32_GIB = "baga6ea4seaqao7s73y24kcutaosvacpdjgfe5pw76ooefnyqw4ynr3d2y6x2mpq"
V2_EMPTY_32_GIB = "bafkzcibcaapao7s73y24kcutaosvacpdjgfe5pw76ooefnyqw4ynr3d2y6x2mpq"
V1_1016 = "baga6ea4seaqn42av3szurbbscwuu3zjssvfwbpsvbjf6y3tukvlgl2nf5rha6pa"
V2_1016 = "bafkzcibcaac542av3szurbbscwuu3zjssvfwbpsvbjf6y3tukvlgl2nf5rha6pa"
V2_EXCESSIVE_PADDING = "bafkzcibdvqbaislnvygmtytf57s2abxiaytklxc4icpf2mkvye4yjsxwzdk47vqf"


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
        expiration=999999,
    ):
        return importer.Allocation(
            allocation_id=allocation_id,
            client=self.CLIENT_ID,
            miner=self.PROVIDER_ID,
            piece_cid=piece_cid,
            piece_size=piece_size,
            term_min=term_min,
            term_max=term_max,
            expiration=expiration,
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
        chain_head=90000,
        start_before_allocation_expiration_epochs=importer.DEFAULT_START_BEFORE_ALLOCATION_EXPIRATION_EPOCHS,
        expected_seal_runway_epochs=importer.DEFAULT_EXPECTED_SEAL_RUNWAY_EPOCHS,
        explicit_start_epoch=None,
    ):
        rows = rows or [self.valid_row()]
        if allocations_by_piece is None:
            allocations_by_piece = {V1_508: [self.allocation()]}
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
                chain_head,
                start_before_allocation_expiration_epochs,
                expected_seal_runway_epochs,
                explicit_start_epoch,
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

    def cli_args(self, *extra, chain_head=90000):
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
            "--chain-head",
            str(chain_head),
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

    def test_piece_cid_v2_rejects_padding_at_or_above_half_capacity(self):
        # Padded size 512, unpadded capacity 508, half capacity 254,
        # encoded padding 300. go-fil-commcid rejects 300 >= 254.
        with self.assertRaisesRegex(ValueError, "less than half"):
            importer.piece_cid_v2_info(V2_EXCESSIVE_PADDING)

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

    def test_nonpositive_allocation_id_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "allocation_id must be positive"):
            self.parse_allocations(
                {"allocations": [self.allocation_record(allocation_id=0)]}
            )

    def test_duration_inside_allocation_range_is_valid(self):
        candidate = self.candidates(duration=200)[0]
        self.assertIsNone(candidate.file_reject_reason)

    def test_default_allocation_derived_start_epoch(self):
        self.assertEqual(
            1440, importer.DEFAULT_START_BEFORE_ALLOCATION_EXPIRATION_EPOCHS
        )
        self.assertEqual(960, importer.DEFAULT_EXPECTED_SEAL_RUNWAY_EPOCHS)
        candidate = self.candidates(
            allocations_by_piece={
                V1_508: [self.allocation(expiration=100000)]
            },
            chain_head=97000,
        )[0]

        self.assertIsNone(candidate.file_reject_reason)
        self.assertEqual(98560, candidate.start_epoch)

    def test_custom_start_before_allocation_expiration_buffer(self):
        candidate = self.candidates(
            allocations_by_piece={
                V1_508: [self.allocation(expiration=100000)]
            },
            chain_head=97000,
            start_before_allocation_expiration_epochs=2000,
        )[0]

        self.assertIsNone(candidate.file_reject_reason)
        self.assertEqual(98000, candidate.start_epoch)

    def test_each_allocation_produces_its_own_start_epoch(self):
        rows = [
            self.valid_row(),
            self.valid_row(piece_cid_v1=V1_508_ALT, piece_cid_v2=V2_508_ALT),
        ]
        allocations = {
            V1_508: [self.allocation(allocation_id=42, expiration=100000)],
            V1_508_ALT: [
                self.allocation(
                    piece_cid=V1_508_ALT,
                    allocation_id=43,
                    expiration=110000,
                )
            ],
        }

        candidates = self.candidates(
            rows=rows,
            allocations_by_piece=allocations,
            chain_head=97000,
        )

        self.assertEqual([98560, 108560], [c.start_epoch for c in candidates])
        self.assertTrue(all(c.file_reject_reason is None for c in candidates))

    def test_explicit_start_epoch_is_batch_wide_and_overrides_fallback_buffer(self):
        rows = [
            self.valid_row(),
            self.valid_row(piece_cid_v1=V1_508_ALT, piece_cid_v2=V2_508_ALT),
        ]
        allocations = {
            V1_508: [self.allocation(allocation_id=42, expiration=105000)],
            V1_508_ALT: [
                self.allocation(
                    piece_cid=V1_508_ALT,
                    allocation_id=43,
                    expiration=115000,
                )
            ],
        }

        candidates = self.candidates(
            rows=rows,
            allocations_by_piece=allocations,
            chain_head=97000,
            explicit_start_epoch=100000,
            start_before_allocation_expiration_epochs=2000,
        )

        self.assertEqual([100000, 100000], [c.start_epoch for c in candidates])
        self.assertTrue(all(c.file_reject_reason is None for c in candidates))
        for candidate in candidates:
            with self.subTest(allocation_id=candidate.allocation_id):
                ddo = json.loads(candidate.ddo_v1_json(self.PROVIDER, 200))
                self.assertEqual(100000, ddo["ddo"]["start_epoch"])
                self.assertIsInstance(ddo["ddo"]["start_epoch"], int)

    def test_explicit_start_epoch_exact_runway_boundary_is_valid(self):
        chain_head = 10000
        runway = 960
        candidate = self.candidates(
            allocations_by_piece={
                V1_508: [self.allocation(expiration=20000)]
            },
            chain_head=chain_head,
            expected_seal_runway_epochs=runway,
            explicit_start_epoch=chain_head + runway,
        )[0]

        self.assertEqual(chain_head + runway, candidate.start_epoch)
        self.assertIsNone(candidate.file_reject_reason)

    def test_explicit_start_epoch_one_below_runway_is_rejected(self):
        chain_head = 10000
        runway = 960
        start_epoch = chain_head + runway - 1
        candidate = self.candidates(
            allocations_by_piece={
                V1_508: [self.allocation(expiration=20000)]
            },
            chain_head=chain_head,
            expected_seal_runway_epochs=runway,
            explicit_start_epoch=start_epoch,
        )[0]

        self.assertEqual(start_epoch, candidate.start_epoch)
        self.assertIn(
            "insufficient sealing runway: "
            f"start_epoch={start_epoch}, minimum_start_epoch={chain_head + runway}, "
            f"chain_head={chain_head}, expected_seal_runway_epochs={runway}",
            candidate.file_reject_reason,
        )
        self.assertIn("mode=explicit", candidate.file_reject_reason)

    def test_explicit_start_epoch_must_precede_each_allocation_expiration(self):
        start_epoch = 100000
        for allocation_expiration, rejected in (
            (start_epoch + 1, False),
            (start_epoch, True),
            (start_epoch - 1, True),
        ):
            with self.subTest(
                allocation_expiration=allocation_expiration,
                rejected=rejected,
            ):
                candidate = self.candidates(
                    allocations_by_piece={
                        V1_508: [
                            self.allocation(expiration=allocation_expiration)
                        ]
                    },
                    chain_head=90000,
                    explicit_start_epoch=start_epoch,
                )[0]

                self.assertEqual(start_epoch, candidate.start_epoch)
                if rejected:
                    self.assertIn(
                        "start epoch is not before allocation expiration: "
                        f"start_epoch={start_epoch}, "
                        f"allocation_expiration={allocation_expiration}",
                        candidate.file_reject_reason,
                    )
                    self.assertIn("mode=explicit", candidate.file_reject_reason)
                else:
                    self.assertIsNone(candidate.file_reject_reason)

    def test_explicit_start_epoch_rejects_only_the_unsafe_allocation(self):
        rows = [
            self.valid_row(),
            self.valid_row(piece_cid_v1=V1_508_ALT, piece_cid_v2=V2_508_ALT),
        ]
        allocations = {
            V1_508: [self.allocation(allocation_id=42, expiration=110000)],
            V1_508_ALT: [
                self.allocation(
                    piece_cid=V1_508_ALT,
                    allocation_id=43,
                    expiration=99999,
                )
            ],
        }

        candidates = self.candidates(
            rows=rows,
            allocations_by_piece=allocations,
            chain_head=90000,
            explicit_start_epoch=100000,
        )

        self.assertIsNone(candidates[0].file_reject_reason)
        self.assertEqual(100000, candidates[0].start_epoch)
        self.assertEqual(100000, candidates[1].start_epoch)
        self.assertIn(
            "start epoch is not before allocation expiration",
            candidates[1].file_reject_reason,
        )

    def test_ddo_json_contains_numeric_nonnull_start_epoch(self):
        candidate = self.candidates()[0]

        ddo = json.loads(candidate.ddo_v1_json(self.PROVIDER, 200))

        self.assertEqual(candidate.start_epoch, ddo["ddo"]["start_epoch"])
        self.assertIsInstance(ddo["ddo"]["start_epoch"], int)
        self.assertIsNotNone(ddo["ddo"]["start_epoch"])

    def test_exact_expected_seal_runway_boundary_is_valid(self):
        chain_head = 10000
        runway = 960
        allocation_expiration = chain_head + runway + 1440

        candidate = self.candidates(
            allocations_by_piece={
                V1_508: [self.allocation(expiration=allocation_expiration)]
            },
            chain_head=chain_head,
            expected_seal_runway_epochs=runway,
        )[0]

        self.assertEqual(chain_head + runway, candidate.start_epoch)
        self.assertIsNone(candidate.file_reject_reason)

    def test_one_epoch_below_expected_seal_runway_is_rejected(self):
        chain_head = 10000
        runway = 960
        start_epoch = chain_head + runway - 1
        candidate = self.candidates(
            allocations_by_piece={
                V1_508: [self.allocation(expiration=start_epoch + 1440)]
            },
            chain_head=chain_head,
            expected_seal_runway_epochs=runway,
        )[0]

        self.assertEqual(start_epoch, candidate.start_epoch)
        self.assertIn(
            "insufficient sealing runway: "
            f"start_epoch={start_epoch}, minimum_start_epoch={chain_head + runway}, "
            f"chain_head={chain_head}, expected_seal_runway_epochs={runway}",
            candidate.file_reject_reason,
        )

    def test_allocation_derived_start_behind_chain_head_is_rejected(self):
        chain_head = 10000
        start_epoch = chain_head - 1
        candidate = self.candidates(
            allocations_by_piece={
                V1_508: [self.allocation(expiration=start_epoch + 1440)]
            },
            chain_head=chain_head,
        )[0]

        self.assertEqual(start_epoch, candidate.start_epoch)
        self.assertIn(
            "allocation-derived start epoch is in the past: "
            f"start_epoch={start_epoch}, chain_head={chain_head}",
            candidate.file_reject_reason,
        )

    def test_nonpositive_allocation_derived_start_is_rejected(self):
        for expected_start in (0, -1):
            with self.subTest(start_epoch=expected_start):
                candidate = self.candidates(
                    allocations_by_piece={
                        V1_508: [
                            self.allocation(expiration=1440 + expected_start)
                        ]
                    },
                    chain_head=1,
                )[0]

                self.assertEqual(expected_start, candidate.start_epoch)
                self.assertIn(
                    "allocation-derived start epoch is non-positive",
                    candidate.file_reject_reason,
                )

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

    def test_nonpositive_chain_head_is_rejected_before_file_access(self):
        for value in (0, -1):
            with self.subTest(chain_head=value):
                with mock.patch.object(
                    importer.sys, "argv", self.cli_args(chain_head=value)
                ):
                    with self.assertRaisesRegex(
                        SystemExit, "--chain-head must be positive"
                    ):
                        importer.main()

    def test_nonpositive_start_buffer_is_rejected_before_file_access(self):
        for value in (0, -1):
            with self.subTest(start_buffer=value):
                with mock.patch.object(
                    importer.sys,
                    "argv",
                    self.cli_args(
                        "--start-before-allocation-expiration-epochs",
                        str(value),
                    ),
                ):
                    with self.assertRaisesRegex(
                        SystemExit,
                        "--start-before-allocation-expiration-epochs must be positive",
                    ):
                        importer.main()

    def test_nonpositive_expected_runway_is_rejected_before_file_access(self):
        for value in (0, -1):
            with self.subTest(expected_runway=value):
                with mock.patch.object(
                    importer.sys,
                    "argv",
                    self.cli_args("--expected-seal-runway-epochs", str(value)),
                ):
                    with self.assertRaisesRegex(
                        SystemExit,
                        "--expected-seal-runway-epochs must be positive",
                    ):
                        importer.main()

    def test_nonpositive_explicit_start_epoch_is_rejected_before_file_access(self):
        for value in (0, -1):
            with self.subTest(explicit_start_epoch=value):
                with mock.patch.object(
                    importer.sys,
                    "argv",
                    self.cli_args("--start-epoch", str(value)),
                ):
                    with self.assertRaisesRegex(
                        SystemExit,
                        "--start-epoch must be positive when specified",
                    ):
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

    def test_non_power_of_two_piece_size_is_rejected_before_file_access(self):
        with mock.patch.object(
            importer.sys, "argv", self.cli_args("--piece-size", "513")
        ):
            with self.assertRaisesRegex(
                SystemExit, "--piece-size must be a positive power of two"
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
            "batch", "audit_mk20_import_stage", 10, "run", 90000, 1440, 960
        )
        self.assertIn("BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE", sql)
        lock_position = sql.index("pg_advisory_xact_lock(1296778320)")
        conflict_check_position = sql.index("CREATE TEMP TABLE picked")
        self.assertLess(lock_position, conflict_check_position)
        self.assertEqual(1, sql.count("pg_advisory_xact_lock(1296778320)"))

    def test_generated_exception_messages_escape_quotes(self):
        sql = importer.generate_insert_sql(
            "batch", "audit_mk20_import_stage", 10, "run'quote", 90000, 1440, 960
        )
        self.assertIn("run''quote", sql)
        self.assertNotIn("run'quote batch", sql)

    def test_validated_csv_contains_start_epoch(self):
        candidate = self.candidates()[0]
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "validated.csv"
            importer.write_candidates_csv(
                path,
                [candidate],
                self.PROVIDER,
                self.PROVIDER_ID,
                "f1client",
                self.CLIENT_ID,
                200,
            )
            with path.open("r", encoding="utf-8", newline="") as handle:
                row = next(csv.DictReader(handle))

        self.assertEqual(str(candidate.start_epoch), row["start_epoch"])
        self.assertEqual(
            candidate.start_epoch,
            json.loads(row["ddo_v1_json"])["ddo"]["start_epoch"],
        )

    def test_stage_sql_carries_start_epoch_migration_safely(self):
        sql = importer.generate_stage_sql(
            Path("validated.csv"),
            "batch",
            "audit_mk20_import_stage",
            False,
            False,
        )
        normalized = " ".join(sql.split())

        self.assertIn("ADD COLUMN IF NOT EXISTS start_epoch BIGINT", normalized)
        self.assertIn("alloc_expiration BIGINT, start_epoch BIGINT", normalized)
        self.assertIn(
            "alloc_term_max, alloc_expiration, start_epoch, file_reject_reason",
            normalized,
        )
        self.assertIn(
            "allocation_id, alloc_term_min, alloc_term_max, alloc_expiration, start_epoch",
            normalized,
        )

    def test_insert_sql_persists_run_and_row_scheduling_audit(self):
        sql = importer.generate_insert_sql(
            "batch", "audit_mk20_import_stage", 10, "run", 123456, 1440, 960
        )
        normalized = " ".join(sql.split())

        for column in (
            "chain_head",
            "explicit_start_epoch",
            "start_before_allocation_expiration_epochs",
            "expected_seal_runway_epochs",
        ):
            with self.subTest(run_column=column):
                self.assertIn(f"ADD COLUMN IF NOT EXISTS {column} BIGINT", normalized)
        for column in ("start_epoch", "alloc_expiration"):
            with self.subTest(inserted_column=column):
                self.assertIn(f"ADD COLUMN IF NOT EXISTS {column} BIGINT", normalized)
        self.assertIn(
            "run_id, batch_name, limit_rows, chain_head, explicit_start_epoch, "
            "start_before_allocation_expiration_epochs, expected_seal_runway_epochs, notes",
            normalized,
        )
        self.assertIn("'123456', NULL, '1440', '960'", normalized)
        self.assertIn(
            "piece_cid_v1, piece_cid_v2, allocation_id, start_epoch, alloc_expiration, car_url",
            normalized,
        )
        self.assertIn("p.start_epoch, p.alloc_expiration, p.car_url", normalized)

    def test_insert_sql_rechecks_start_epoch_policy_before_insert(self):
        sql = importer.generate_insert_sql(
            "batch", "audit_mk20_import_stage", 10, "run", 123456, 1440, 960
        )
        guard_position = sql.index("Unsafe StartEpoch scheduling metadata")
        insert_position = sql.index("INSERT INTO market_mk20_deal")

        self.assertLess(guard_position, insert_position)
        self.assertIn(
            "jsonb_typeof(s.ddo_v1_json #> '{ddo,start_epoch}') IS DISTINCT FROM 'number'",
            sql,
        )
        self.assertIn(
            "s.ddo_v1_json #>> '{ddo,start_epoch}' IS DISTINCT FROM s.start_epoch::TEXT",
            sql,
        )
        self.assertIn("(s.alloc_expiration > s.start_epoch) IS NOT TRUE", sql)
        self.assertIn(
            "(s.alloc_expiration - s.start_epoch = 1440) IS NOT TRUE", sql
        )
        self.assertIn("(s.start_epoch >= 123456 + 960) IS NOT TRUE", sql)

    def test_explicit_insert_sql_persists_and_enforces_only_explicit_policy(self):
        sql = importer.generate_insert_sql(
            "batch",
            "audit_mk20_import_stage",
            10,
            "run",
            123456,
            1440,
            960,
            200000,
        )
        normalized = " ".join(sql.split())

        self.assertIn("'123456', 200000, '1440', '960'", normalized)
        self.assertIn("s.start_epoch IS DISTINCT FROM 200000", sql)
        self.assertNotIn(
            "s.alloc_expiration - s.start_epoch = 1440",
            sql,
        )
        self.assertIn("(s.alloc_expiration > s.start_epoch) IS NOT TRUE", sql)
        self.assertIn("(s.start_epoch >= 123456 + 960) IS NOT TRUE", sql)

    def test_fallback_insert_sql_retains_allocation_buffer_policy(self):
        sql = importer.generate_insert_sql(
            "batch", "audit_mk20_import_stage", 10, "run", 123456, 1440, 960
        )

        self.assertIn(
            "(s.alloc_expiration - s.start_epoch = 1440) IS NOT TRUE",
            sql,
        )
        self.assertNotIn("s.start_epoch IS DISTINCT FROM 200000", sql)

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

    def test_verify_sql_checks_all_start_epoch_relationships(self):
        sql = importer.generate_verify_sql(
            "batch", "audit_mk20_import_stage", "run"
        )

        for problem in (
            "bad_ddo_start_epoch",
            "bad_allocation_start_order",
            "bad_allocation_start_buffer",
            "bad_explicit_start_epoch",
            "insufficient_audited_start_runway",
        ):
            with self.subTest(problem=problem):
                self.assertIn(f"SELECT '{problem}' AS problem", sql)
        self.assertIn("jsonb_typeof(d.ddo_v1 #> '{ddo,start_epoch}')", sql)
        self.assertIn(
            "d.ddo_v1 #>> '{ddo,start_epoch}' IS DISTINCT FROM i.start_epoch::TEXT",
            sql,
        )
        self.assertIn("i.alloc_expiration <= i.start_epoch", sql)
        self.assertIn(
            "IS DISTINCT FROM r.start_before_allocation_expiration_epochs",
            sql,
        )
        self.assertIn("r.explicit_start_epoch IS NULL", sql)
        self.assertIn("r.explicit_start_epoch IS NOT NULL", sql)
        self.assertIn(
            "i.start_epoch IS DISTINCT FROM r.explicit_start_epoch",
            sql,
        )
        self.assertIn(
            "i.start_epoch < r.chain_head + r.expected_seal_runway_epochs",
            sql,
        )
        self.assertIn("i.start_epoch, i.alloc_expiration", sql)

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

    def test_observe_sql_includes_run_policy_and_row_schedule(self):
        sql = importer.generate_observe_sql("batch", "run")

        self.assertIn("SELECT 'run_policy' AS section", sql)
        self.assertIn("chain_head", sql)
        self.assertIn("explicit_start_epoch", sql)
        self.assertIn("start_before_allocation_expiration_epochs", sql)
        self.assertIn("expected_seal_runway_epochs", sql)
        self.assertIn("i.start_epoch, i.alloc_expiration", sql)


if __name__ == "__main__":
    unittest.main()
