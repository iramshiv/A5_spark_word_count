
# coding: utf-8

# In[4]:


#!pip install wget

import wget
from pyspark import SparkConf
from pyspark import SparkContext

# initialize and configure a basic SparkContext.
config = SparkConf().setMaster('spark://localhost:7077').setAppName('App')
sconfig = SparkContext(conf=config)

# Download shakespheare file
url = "https://ocw.mit.edu/ans7870/6/6.006/s08/lecturenotes/files/t8.shakespeare.txt"
wget.download(url, '/srv/spark/s.txt')

readFile = open("/srv/spark/s.txt")
writeFile = open("/srv/spark/file.txt", 'w')

lines = readFile.readlines()

# Header ends and Shakespheare writing starts from line 250
for i in range(250, len(lines)):
        
        line = lines[i]
        
        # Removing unrelevant data inbetween shakespheare writing
        if line.startswith('<<') or line.startswith('SHAKESPEARE') or line.startswith('WITH') or         line.startswith('DISTRIBUTED') or line.startswith('PERSONAL') or line.startswith('COMMERCIALLY') or         line.startswith('SERVICE') or line.startswith('PROVIDED'):
            qe = line
            qe =''
        else :
            writeFile.write(line) # file contains only the shakespheare writing

writeFile.close()           
readFile.close()

# load the shakespheare text file
text_file = sconfig.textFile("/srv/spark/file.txt")

# count the no. of times each word appears on the file
counts = (text_file.flatMap(lambda line: line.split(' ')).filter(lambda word: word!="")
                   .map(lambda word: (word, 1))
                   .reduceByKey(lambda a, b: a + b))

# top 24 words
counts.takeOrdered(24, key=lambda x: -x[1])


# In[248]:


# cleansed shakespheare file

filename = '/srv/spark/file.txt'
infile = open(filename, 'r') 
lines = infile.readlines()

for line in lines:
    print(line.strip())


# *notebook writen by [fscm](https://github.com/fscm)*
