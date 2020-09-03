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

pinoc = sc.textFile("pinocch.txt")
# sample first 10 lines
print(pinoc.take(10))

# compute a new dataset of line lengths and sum them
lengths = pinoc.map(lambda s: len(s))
print(lengths.reduce(lambda a, b: a + b))

# split up into words
word_pinoc = pinoc.flatMap(lambda line: line.split(" "))

# find word with greatest length
print(word_pinoc.reduce(lambda w1, w2: w1 if len(w1) > len(w2) else w2))

# get unique words
word_unique_pinoc = word_pinoc.distinct()

# words that start with s
word_s_pinoc.filter(lambda w: w.startswith("s"))

char_pinoc = word_e.flatMap(lambda word: list(word))
total_char = char_pinoc.count()
freq_pinoc = pinoc.countByValue()
