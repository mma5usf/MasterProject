__author__ = 'mingluma'

from pyspark.sql import SQLContext, Row
from pyspark import SparkContext
import sys
from timeit import default_timer
"""
usage:
python sparkSQL_benchmark.py scan <x>
python sparkSQL_benchmark.py aggregation <x>
python sparkSQL_benchmark.py join <date>
"""

"""
This project load data from hdfs, create table and query form these tables.
"""
class spark_sql_benchmark():
    def __init__(self):
        self.sc = SparkContext()
        self.sqlContext = SQLContext(self.sc)
    """
    automatically create the temp tables and print out the message when the tables are ready
    """
    def create_table_rankings(self):
        path = "amplab/text/tiny/rankings"
        lines = self.sc.textFile(path)
        # DEBUG: x = lines.take(10)
        parts = lines.map(lambda l: l.split(","))
        rankings = parts.map(lambda r: Row(pageURL = r[0], pageRank = int(r[1]), avgDuration = int(r[2])))
        # create table ranking
        # Infer the schema, and register the SchemaRDD as a table.
        schemaRanking = self.sqlContext.inferSchema(rankings)
        schemaRanking.registerTempTable("rankings")
        print "The table rankings is created sucessfully!"

    # create table uservisits
    def create_table_user(self):
        path = "amplab/text/tiny/uservisits"
        lines = self.sc.textFile(path)
        parts = lines.map(lambda l: l.split(","))
        uservisits = parts.map(lambda r: Row(sourceIP = r[0], destURL = r[1],
        visitDate = r[2], adRevenue = float(r[3]), userAgent = r[4], countryCode = r[5],
        languageCode = r[6], searchWord = r[7], duration = int(r[8])))
        schemaRanking = self.sqlContext.inferSchema(uservisits)
        schemaRanking.registerTempTable("uservisits")
        print "The table uservisits is created sucessfully"

    # query one
    def scan(self, x):
        scan = self.sqlContext.sql("SELECT pageURL, pageRank FROM rankings WHERE pageRank > " + x)
        # print the result
        scan_result = scan.collect()
        for s in scan_result:
            print s

    # query two
    def aggregation(self, x):
        aggre = self.sqlContext.sql("SELECT SUBSTR(sourceIP, 1, "+ x +
                               "), SUM(adRevenue) FROM uservisits GROUP BY SUBSTR(sourceIP, 1, " + x + ")")
        result = aggre.collect()
        for r in result:
            print r

    # query three
    def join(self, date):
        joined = self.sqlContext.sql("SELECT sourceIP, totalRevenue, avgPageRank FROM  (SELECT sourceIP, "
                                "AVG(pageRank) as avgPageRank, SUM(adRevenue) as totalRevenue FROM "
                                "rankings AS R, uservisits AS UV WHERE R.pageURL = UV.destURL AND "
                                "UV.visitDate BETWEEN '1980-01-01' AND '" + date + " ' GROUP BY UV.sourceIP) "
                                "tb ORDER BY totalRevenue DESC LIMIT 1")
        result = joined.collect()
        for r in result:
            print r


def main(argv):
    if len(argv) < 3:
        print argv
        print "Need Three Arguments"
        print "scan/aggregation/join x"
        return

    command = argv[1]
    x = argv[2]
    spark_sql = spark_sql_benchmark()


    if command == "scan":
        spark_sql.create_table_rankings()
        if x.isdigit():
            start = default_timer()
            spark_sql.scan(x)
            duration = default_timer() - start
            print "Duration time is %f" %duration
        else:
            print "The argv after scan should be an integer!"
            return
    elif command == "aggregation":
        if x.isdigit() and int(x) > 0:
            spark_sql.create_table_user()
	    start = default_timer()
            spark_sql.aggregation(x)
            duration = default_timer() - start
            print "Duration time is %f" %duration
        else:
            print "The argv after aggregation should be an integer bigger than 0! "
            return

    elif command == "join":
        if not "-" in x and len(x) != 10:
            print "The argv after join should be a date in format: yyyy-mm-dd !"
            return
        else:
            spark_sql.create_table_rankings()
            spark_sql.create_table_user()
            start = default_timer()
            spark_sql.join(x)
            duration = default_timer() - start
            print "Duration time is %f" %duration
    else:
        print "Invalid volume"
        return

if __name__ == "__main__":
    main(sys.argv)