import apache_beam as beam


class Split(beam.DoFn):
    """DoFn which splits each each element by ","
    and converts 2 second element to a float"""

    def process(self, element):
        try:
            driver, time = element.split(",")
            return [
                (
                    driver,
                    float(time),
                )
            ]
        except ValueError:
            # logging.error("CSV does not follow the expected schema of string,float")
            raise ValueError("CSV does not follow the expected schema of string,float")
        # return tuple(element.split(","))


# class CollectTimes(beam.DoFn):
#     def process(self, element):
#         # Returns a list of tuples containing the 1 key and time value
#         result = [(1, element['time'])]
#         return result
#
# class FormatCSV(beam.DoFn):
#     def process(self, element):
#         # Returns a list of tuples containing the 1 key and time value
#         rows = []
#         for record in element:
#             row = ','.join([str(x) for x in record])
#             # rows.append(','.join([str(x) for x in record]))
#             rows.append(row)
#         result = '\n'.join(rows)
#         return result


class MapFunctions:
    """General functions to be called by beam.Map"""

    def addKey(row):
        """Inserts '1' key to each element in order to group globally

        :return: Tuple containing 2 elements, 1 and a tuple of the row"""
        return (1, row)

    def sortGroupedData(row):
        """Sorts a list by the second element of each record & filter for top 3

        *Note: The PCollection is condensed into a single element

        :return: List of Tuples, where each tuple is a record"""
        (keyNumber, sortData) = row
        sortData.sort(key=lambda x: x[1], reverse=False)
        return sortData[:3]

    def formatCSV(row):
        """Formats the records into a CSV format.
        Applied as a map since the global group and sort results in a
        single element in the PCollection.

        :return: Multiline string corresponding to the CSV output"""
        rows = []
        for record in row:
            line = ",".join([str(x) for x in record])
            # rows.append(','.join([str(x) for x in record]))
            rows.append(line)
        result = "\n".join(rows)
        return result
