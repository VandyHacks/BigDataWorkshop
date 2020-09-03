import pyspark

conf = pyspark.conf.SparkConf()
conf.setAppName("big-data-workshop").setMaster("local")
sc: pyspark.SparkContext = pyspark.context.SparkContext.getOrCreate(conf=conf)

no_e = sc.textFile("no-e.txt")
word_e = no_e.flatMap(lambda line: line.split(" "))
char_e = word_e.flatMap(lambda word: list(word))

freq_e = char_e.countByValue()
# the number of 'e's in the text is actually 4
print(freq_e['e'])