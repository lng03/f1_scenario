import apache_beam as beam
import unittest
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from helpers.beam_helpers import Split
from helpers.beam_helpers import MapFunctions


class TestSplit(unittest.TestCase):
    def test_split(self):
        """Tests split function correctly splits and applies type conversion"""
        with TestPipeline() as p:
            test_input = p | "create" >> beam.Create(["Alonzo,4.32"])

            mapped_output = test_input | "Split CSV data" >> beam.ParDo(Split())

            assert_that(
                mapped_output,
                equal_to([("Alonzo", 4.32)]),
            )

    def test_split_failure(self):
        """Tests split function correctly raises ValueErrors where schema
        does not match"""
        with self.assertRaises(ValueError):
            with TestPipeline() as p:
                (
                    p
                    | "create" >> beam.Create(["Alonzo"])
                    | "Split CSV data" >> beam.ParDo(Split())
                )


class TestFormatCSV(unittest.TestCase):
    def test_format_csv(self):
        """Tests that the format CSV function is correctly applied as a map"""
        with TestPipeline() as p:
            test_input = p | "create" >> beam.Create(
                [
                    [
                        ("Verstrappen", 4.63),
                        ("Hamilton", 4.5633333333333335),
                        ("Alonzo", 4.526666666666666),
                    ]
                ]
            )

            formatted_csv = test_input | "Format CSV" >> beam.Map(
                MapFunctions.formatCSV
            )

            assert_that(
                formatted_csv,
                equal_to(
                    [
                        # (1, 4.32), (1, 4.88)
                        "Verstrappen,4.63\n"
                        "Hamilton,4.5633333333333335\n"
                        "Alonzo,4.526666666666666"
                    ]
                ),
            )


class TestMapFunctions(unittest.TestCase):
    def test_addKey(self):
        """Tests that addKey correctly forms a 2 element tuple
        containing 1 and the row"""
        row = ("val1", "val2")

        assert MapFunctions.addKey(row) == (1, ("val1", "val2"))

    def test_sortGroupedData(self):
        """Tests that sortGroupedData correctly sorts in ascending
        order and filters to top 3"""
        row = (
            1,
            [
                ("Hamilton", 1.1),
                ("Verstrappen", 1.2),
                ("Alonzo", 1.3),
                ("Vettel", 1.5),
            ],
        )

        assert MapFunctions.sortGroupedData(row) == [
            ("Hamilton", 1.1),
            ("Verstrappen", 1.2),
            ("Alonzo", 1.3),
        ]

    def test_formatCSV(self):
        """Tests that formatCSV correctly formats to CSV"""
        row = [
            ("Verstrappen", 4.63),
            ("Hamilton", 4.5633333333333335),
            ("Alonzo", 4.526666666666666),
        ]

        expected = (
            "Verstrappen,4.63\n"
            "Hamilton,4.5633333333333335\n"
            "Alonzo,4.526666666666666"
        )

        actual = MapFunctions.formatCSV(row)

        assert actual == expected


if __name__ == "__main__":
    unittest.main()
