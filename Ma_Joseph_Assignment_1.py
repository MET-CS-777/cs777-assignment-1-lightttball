from __future__ import print_function

import sys
from pyspark import SparkContext


# -------------------------------------------------
# Exception Handling
# -------------------------------------------------
def isfloat(value):
    try:
        float(value)
        return True
    except:
        return False


# -------------------------------------------------
# Data Cleaning
# -------------------------------------------------
def correctRows(p):

    if len(p) != 17:
        return False

    if not (
        isfloat(p[4]) and
        isfloat(p[5]) and
        isfloat(p[11]) and
        isfloat(p[16])
    ):
        return False

    return (
        float(p[4]) > 60 and
        float(p[5]) > 0 and
        float(p[11]) > 0 and
        float(p[16]) > 0
    )


# -------------------------------------------------
# Main
# -------------------------------------------------
if __name__ == "__main__":

    if len(sys.argv) != 4:
        print("Usage: main_task1 <input> <output_task1> <output_task2>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="Assignment-1")

    # Read input
    rdd = sc.textFile(sys.argv[1])

    # Clean data
    cleanData = (
        rdd.map(lambda line: line.split(","))
           .filter(correctRows)
    )

    # ==================================================
    # Task 1
    # Top 10 taxis with the highest number of distinct drivers
    # ==================================================

    results_1 = (
        cleanData
        .map(lambda x: (x[0], x[1]))
        .distinct()
        .map(lambda x: (x[0], 1))
        .reduceByKey(lambda a, b: a + b)
        .sortBy(lambda x: x[1], ascending=False)
        .take(10)
    )

    sc.parallelize(results_1) \
        .coalesce(1) \
        .saveAsTextFile(sys.argv[2])


    # ==================================================
    # Task 2
    # Top 10 drivers with the highest average earning per minute
    # ==================================================

    results_2 = (
        cleanData
        .map(
            lambda x: (
                x[1],
                (float(x[16]), float(x[4]))
            )
        )
        .reduceByKey(
            lambda a, b: (
                a[0] + b[0],
                a[1] + b[1]
            )
        )
        .mapValues(
            lambda x: x[0] / (x[1] / 60.0)
        )
        .sortBy(lambda x: x[1], ascending=False)
        .take(10)
    )

    sc.parallelize(results_2) \
        .coalesce(1) \
        .saveAsTextFile(sys.argv[3])


    # ==================================================
    # Task 3 (Optional)
    # ==================================================


    # ==================================================
    # Task 4 (Optional)
    # ==================================================


    sc.stop()