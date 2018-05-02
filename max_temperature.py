from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("max_temper_per_station")
sc = SparkContext(conf=conf)

def parse_lines(line):
	fields = line.split(',')
	stationID = fields[0]
	entryType = fields[2]
	# temperature = float(fields[3])
	temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
	return (stationID, entryType, temperature)

rdd = sc.textFile("/Users/omkar/PersonalProjects/Spark-Udemy/1800.csv")
# parsed_rdd = rdd.map(parse_lines)
# only_max = parsed_rdd.filter(lambda x: "TMAX" in x[1])
# relevant_fields = only_max.map(lambda x: (x[0], x[2]))
# max_temps = relevant_fields.reduceByKey(lambda x,y: max(x,y))

parsedLines = rdd.map(parse_lines)
maxTemps = parsedLines.filter(lambda x: "TMAX" in x[1])
stationTemps = maxTemps.map(lambda x: (x[0], x[2]))
maxTemps = stationTemps.reduceByKey(lambda x, y: max(x, y))
results = maxTemps.collect()

for result in results:
	print(result)