import apache_beam as beam
import argparse
import logging
from apache_beam.options.pipeline_options import PipelineOptions

from helpers.beam_helpers import Split
from helpers.beam_helpers import MapFunctions

# Command line arguments
parser = argparse.ArgumentParser()
parser.add_argument(
    "--input", help="Input for the pipeline", default="./data/laptimes.csv"
)
parser.add_argument(
    "--output", help="Output for the pipeline", default="./output/avg_laptimes.csv"
)
known_args, pipeline_args = parser.parse_known_args()


def run(input=known_args.input, output=known_args.output):
    # instantiate the pipeline
    options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=options) as p:

        # calculate the mean time for each driver
        mean_times = (
            p
            | beam.io.ReadFromText(input, skip_header_lines=0)
            | beam.ParDo(Split())
            | "Grouping keys Open" >> beam.GroupByKey()
            | "Calculating mean for Open"
            >> beam.CombineValues(beam.combiners.MeanCombineFn())
        )

        # Globally sort the PCollection & filter to top 3
        # Filter is applied within sort in order maintain performance
        # as the global group restricts to workload to a single worker

        sorted_means = (
            mean_times
            | "AddKey" >> beam.Map(MapFunctions.addKey)
            | "GroupByKey" >> beam.GroupByKey()
            | "SortGroupedData" >> beam.Map(MapFunctions.sortGroupedData)
        )

        # Format to CSV and write to file
        output = (
            sorted_means
            | "Format CSV" >> beam.Map(MapFunctions.formatCSV)
            | beam.io.WriteToText(output, shard_name_template="")
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
