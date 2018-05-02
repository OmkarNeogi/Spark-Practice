from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("friends_by_age_test")
sc = SparkContext(conf=conf)

def parse_file(line):
	_, _, x, y = line.split(',')
	return int(x), int(y)

rdd = sc.textFile("/Users/omkar/PersonalProjects/Spark-Udemy/fakefriends.csv")
basic_kv = rdd.map(parse_file)
intermed_rdd = basic_kv.mapValues(lambda x: (x, 1))
totals_by_age = intermed_rdd.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
averages_by_age = totals_by_age.mapValues(lambda x: x[0]//x[1])
results = averages_by_age.collect()
for result in results:
	print(result)