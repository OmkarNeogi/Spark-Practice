from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("min_temp_1800_test")
sc = SparkContext(conf=conf)

def parse_line(line):
	fields = line.split(',')
	stationID = fields[0]
	entryType = fields[2]
	# temperature = float(fields[3])
	temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32

	return stationID, entryType, temperature

rdd = sc.textFile("/Users/omkar/PersonalProjects/Spark-Udemy/1800.csv")

parsed_lines = rdd.map(parse_line)
only_min = parsed_lines.filter(lambda x: "TMIN" in x[1])
stationTemps = only_min.map(lambda x: (x[0], x[2]))
minTemps = stationTemps.reduceByKey(lambda x, y: min(x, y))

results = minTemps.collect()
for result in results:
	print(result[0] + "\t{:.2f}F".format(result[1]))