import unittest
import process
import tempfile
import shutil
import os
import csv


class SmokeTest(unittest.TestCase):
    """General smoke test that executes the pipeline"""

    def test_smoke(self):
        dirpath = tempfile.mkdtemp()
        output = os.path.join(dirpath, "avg_laptimes.csv")
        process.run(output=output)

        rows = []
        with open(output) as csvfile:
            csv_reader = csv.DictReader(csvfile, ["driver", "time"])
            for row in csv_reader:
                rows.append(row)

        expected = [
            {"driver": "Alonzo", "time": "4.526666666666666"},
            {"driver": "Hamilton", "time": "4.5633333333333335"},
            {"driver": "Verstrappen", "time": "4.63"},
        ]
        assert rows == expected
        shutil.rmtree(dirpath)


if __name__ == "__main__":
    unittest.main()
