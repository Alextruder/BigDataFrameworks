# 2. Tutorial Spark

## 2.1 Hints

### 2.1.1 Launch a Spark job

```
spark-submit <python-file>
```

### 2.1.2 Write a python class

``` py
class MyClass(object):
    # Constructor
    def __init__ (self,param1,param2):
        self.member1 = param1
        self.member2 = param2
    # equivalent to toString
    def __str__ (self):
        return " MyClass['%s','%s']"%(self.member1,self.member2)
    # get method
    def getMember1(self):
        return self.member1
instance = MyClass("pi",3.1415)
print instance
print instance.getMember1()
```

We use `spark-submit --master=yarn class.py` to run our file:

```
[alexandre.vignaud@hadoop-edge01 TP3_Spark]$ spark-submit --master=yarn  class.py
21/12/01 16:11:25 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
21/12/01 16:11:25 INFO util.ShutdownHookManager: Shutdown hook called
21/12/01 16:11:25 INFO util.ShutdownHookManager: Deleting directory /tmp/spark-000db266-6b45-4c68-8690-a01b46ae68f3
 MyClass['pi','3.1415']
pi
```

## 2.2 Number of trees

```py
#!/usr/bin/env python
from pyspark import SparkConf, SparkContext
appName = "numberOfTrees"
conf = SparkConf().setAppName(appName)
sc = SparkContext(conf=conf)
# Open the file
file = sc.textFile("hdfs:/user/alexandre.vignaud/trees.csv")
# Display the number of lines
number = file.count ()
print "Number of lines:", number
```

We use `spark-submit --master=yarn job.py` to run our code: 
```bash
[alexandre.vignaud@hadoop-edge01 TP3_Spark]$ spark-submit --master=yarn  job.py
21/12/01 16:22:49 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
21/12/01 16:22:50 INFO spark.SparkContext: Running Spark version 2.4.7.1.0.3.0-223
21/12/01 16:22:50 INFO spark.SparkContext: Submitted application: numberOfTrees
21/12/01 16:22:50 INFO spark.SecurityManager: Changing view acls to: alexandre.vignaud
21/12/01 16:22:50 INFO spark.SecurityManager: Changing modify acls to: alexandre.vignaud
[...]
Number of lines: 98
[...]
21/12/01 16:23:16 INFO spark.SparkContext: Successfully stopped SparkContext
21/12/01 16:23:16 INFO util.ShutdownHookManager: Shutdown hook called
21/12/01 16:23:16 INFO util.ShutdownHookManager: Deleting directory /tmp/spark-d5fcea17-3805-4874-a4be-72913db16eb2
21/12/01 16:23:16 INFO util.ShutdownHookManager: Deleting directory /tmp/spark-cdf7b731-05c3-47d0-90fd-d826a843dd5f
21/12/01 16:23:16 INFO util.ShutdownHookManager: Deleting directory /tmp/spark-d5fcea17-3805-4874-a4be-72913db16eb2/pyspark-90998723-355e-4f8e-a9e0-909cb4616569
```

In this answer, the important information we need to see is `Number of lines: 98`

## 2.3 Average tree height
treeMain.py
```py
from Tree import Tree
from pyspark import SparkConf , SparkContext

appName = "AverageTreeHeight"
conf = SparkConf().setAppName(appName)
sc = SparkContext (conf = conf)

# Display the number of lines
rdd = sc.textFile ("hdfs:/user/alexandre.vignaud/trees.csv")
instance = Tree(rdd)
instance.separaterdd()
instance.takecolumnrdd()
hauteur, _ = instance.filteredcolumn()
instance.sumcolumn()
instance.calcmean(hauteur)
```

Tree.py
```py
class Tree(object):

    def __init__(self,file):
        self.file = file

    def separaterdd(self):
        self.file = self.file.map(lambda l : l.split(";"))
        return self.file

    def takecolumnrdd(self):
        self.file = self.file.map(lambda x: x[6])
        return self.file

    def filteredcolumn(self):
        self.file = self.file.filter(lambda x: x != 'HAUTEUR' ).filter(lambda x : x != '')
        return self.file.count(), self.file

    def sumcolumn(self):
        self.file = self.file.map(lambda y : float(y)).sum()
        return self.file

    def calcmean(self, hauteur):
        print "\nAverage Tree Height :", self.file/hauteur, "\n"
```

### Answer

We use `spark-submit --master=yarn treeMain.py` to run our code: 

```bash
[alexandre.vignaud@hadoop-edge01 TP3_Spark]$ spark-submit --master=yarn treeMain.py
21/12/08 14:42:32 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
21/12/08 14:42:32 INFO spark.SparkContext: Running Spark version 2.4.7.1.0.3.0-223
21/12/08 14:42:32 INFO spark.SparkContext: Submitted application: AverageTreeHeight
21/12/08 14:42:32 INFO spark.SecurityManager: Changing view acls to: alexandre.vignaud
21/12/08 14:42:32 INFO spark.SecurityManager: Changing modify acls to: alexandre.vignaud
[...]
Average Tree Height : 22.3125
[...]
21/12/08 14:43:02 INFO util.ShutdownHookManager: Shutdown hook called
21/12/08 14:43:02 INFO util.ShutdownHookManager: Deleting directory /tmp/spark-984b84c5-64f6-416b-9268-354d99b81e37/pyspark-e9ff09cd-4dac-4ec3-9689-f0f01ab3214b
21/12/08 14:43:02 INFO util.ShutdownHookManager: Deleting directory /tmp/spark-984b84c5-64f6-416b-9268-354d99b81e37
21/12/08 14:43:02 INFO util.ShutdownHookManager: Deleting directory /tmp/spark-fe2c19dc-cd1a-4343-805c-b479039edad9
```

## 2.4 Other jobs
other.py
```py
from pyspark import SparkConf, SparkContext

appName = "numberOfTrees"
conf = SparkConf().setAppName(appName)
sc = SparkContext (conf = conf)
originalrdd = sc.textFile ("hdfs:/user/alexandre.vignaud/trees.csv")
rdd = originalrdd.map(lambda l: l.split(";"))

# Displays the list of district containing trees
rdd1 = rdd.map(lambda x: x[1])
rdd1 = rdd1.filter(lambda x: x!='ARRONDISSEMENT').filter(lambda x : x)
rdd1 = rdd1.distinct().collect()
print "\n"," District: ",rdd1,"\n"

# Displays the list of different species trees
rdd2 = rdd.map(lambda x: x[3])
rdd2 = rdd2.filter(lambda x: x!='ESPECE').filter(lambda x : x)
rdd2 = rdd2.distinct().collect()
print "\n"," Species: ",rdd2,"\n"

# The number of trees of each kind
rdd3 = rdd.map(lambda x : x[3])
rdd3 = rdd3.filter(lambda x: x!='ESPECE').filter(lambda x : x)
rdd3 = rdd3.map(lambda word : (word, 1)).reduceByKey(lambda a,b : a+b)
rdd3 = rdd3.collect()
print "\n"," Number of trees by species: ",rdd3,"\n"

# Calculates the height of the tallest tree of each kind
rdd4 = rdd.map(lambda x: (x[3],x[6]))
rdd4 = rdd4.filter(lambda x: x[0]!='ESPECE' or x[1]!='HAUTEUR').filter(lambda x : x[1] != '')
rdd4 = rdd4.map(lambda x: (x[0], float(x[1])))
rdd4 = rdd4.filter(lambda x: max((x))).reduceByKey(max)
rdd4 = rdd4.collect()
print "\n"," Height of the tallest trees by species: ",rdd4,"\n"

# Sort the trees height from smallest to largest
rdd5 = rdd.map(lambda x: x[6])
rdd5 = rdd5.filter(lambda x: x !='HAUTEUR').filter(lambda x : x)
rdd5 = rdd5.sortBy(lambda x: float(x)).collect()
print "\n"," Trees sort by height: ",rdd5,"\n"
#  Displays the district where the oldest tree is
rdd6 = rdd.map(lambda x: (x[1],x[5]))
rdd6 = rdd6.filter(lambda x: x[0]!='ARRONDISSEMENT' or x[1]!='ANNEE PLANTATION')
rdd6 = rdd6.filter(lambda x: min(x)).reduceByKey(min)
rdd6 = rdd6.sortBy(lambda x: int(x[1]))
rdd6 = rdd6.first()
print "\n"," District of the oldest tree: ",rdd6,"\n"

# Displays the district that contains the most trees
rdd7 = rdd.map(lambda x: x[1])
rdd7 = rdd7.filter(lambda x: x!='ARRONDISSEMENT').filter(lambda x : x)
rdd7 = rdd7.map(lambda word : (word, 1)).reduceByKey(lambda a,b : a+b)
rdd7 = rdd7.sortBy(lambda x: int(x[1]),ascending=False)
rdd7 = rdd7.first()
print "\n"," District of highest number of trees: ",rdd7, "\n"
```

### Answer
We use `spark-submit --master=yarn other.py` to run our code: 

```bash
[alexandre.vignaud@hadoop-edge01 TP3_Spark]$ spark-submit --master=yarn other.py
21/12/08 15:06:22 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
21/12/08 15:06:22 INFO spark.SparkContext: Running Spark version 2.4.7.1.0.3.0-223
21/12/08 15:06:22 INFO spark.SparkContext: Submitted application: numberOfTrees
21/12/08 15:06:22 INFO spark.SecurityManager: Changing view acls to: alexandre.vignaud
21/12/08 15:06:22 INFO spark.SecurityManager: Changing modify acls to: alexandre.vignaud
21/12/08 15:06:22 INFO spark.SecurityManager: Changing view acls groups to:
[...]
 District:  [u'11', u'13', u'20', u'17', u'19', u'3', u'5', u'7', u'9', u'15', u'12', u'14', u'16', u'18', u'4', u'6', u'8']
[...]
 Species:  [u'pseudoacacia', u'ilex', u'baccata', u'fraxinifolia', u'virginiana', u'nigra', u'kaki', u'suber', u'orientalis', u'ulmoides', u'opalus', u'decurrens', u'atlantica', u'giganteum', u'bignonioides', u'grandiflora', u'araucana', u'monspessulanum', u'libanii', u'giraldii', u'coulteri', u'stenoptera', u'tomentosa', u'biloba', u'hippocastanum', u'distichum', u'japonicum', u'excelsior', u'glutinosa', u'nigra laricio', u'bungeana', u'x acerifolia', u'involucrata', u'petraea', u'serrata', u'sylvatica', u'australis', u'cappadocicum', u'pomifera', u'dioicus', u'carpinifolia', u'sempervirens', u'tulipifera', u'papyrifera', u'colurna']
[...]
 Number of trees by species:  [(u'pseudoacacia', 1), (u'ilex', 1), (u'baccata', 2), (u'fraxinifolia', 2), (u'virginiana', 2), (u'nigra', 3), (u'kaki', 2), (u'suber', 1), (u'orientalis', 8), (u'ulmoides', 1), (u'opalus', 1), (u'decurrens', 1), (u'atlantica', 2), (u'giganteum', 5), (u'bignonioides', 1), (u'grandiflora', 1), (u'araucana', 1), (u'monspessulanum', 1), (u'libanii', 2), (u'giraldii', 1), (u'coulteri', 1), (u'stenoptera', 1), (u'tomentosa', 2), (u'biloba', 5), (u'hippocastanum', 3), (u'distichum', 3), (u'japonicum', 1), (u'excelsior', 1), (u'glutinosa', 1), (u'nigra laricio', 1), (u'bungeana', 1), (u'x acerifolia', 11), (u'involucrata', 1), (u'petraea', 2), (u'serrata', 1), (u'sylvatica', 8), (u'australis', 1), (u'cappadocicum', 1), (u'pomifera', 1), (u'dioicus', 1), (u'carpinifolia', 4), (u'sempervirens', 1), (u'tulipifera', 2), (u'papyrifera', 1), (u'colurna', 3)]
[...]
 Height of the tallest trees by species:  [(u'pseudoacacia', 11.0), (u'ilex', 15.0), (u'baccata', 13.0), (u'fraxinifolia', 27.0), (u'virginiana', 14.0), (u'nigra', 30.0), (u'kaki', 14.0), (u'suber', 10.0), (u'orientalis', 34.0), (u'ulmoides', 12.0), (u'opalus', 15.0), (u'decurrens', 20.0), (u'atlantica', 25.0), (u'giganteum', 35.0), (u'bignonioides', 15.0), (u'grandiflora', 12.0), (u'araucana', 9.0), (u'monspessulanum', 12.0), (u'libanii', 30.0), (u'giraldii', 35.0), (u'coulteri', 14.0), (u'stenoptera', 30.0), (u'tomentosa', 20.0), (u'biloba', 33.0), (u'hippocastanum', 30.0), (u'distichum', 35.0), (u'japonicum', 10.0), (u'excelsior', 30.0), (u'glutinosa', 16.0), (u'nigra laricio', 30.0), (u'bungeana', 10.0), (u'x acerifolia', 45.0), (u'involucrata', 12.0), (u'petraea', 31.0), (u'serrata', 18.0), (u'sylvatica', 30.0), (u'australis', 16.0), (u'cappadocicum', 16.0), (u'pomifera', 13.0), (u'dioicus', 10.0), (u'carpinifolia', 30.0), (u'sempervirens', 30.0), (u'tulipifera', 35.0), (u'papyrifera', 12.0), (u'colurna', 20.0)]
[...]
 Trees sort by height:  [u'2.0', u'5.0', u'6.0', u'9.0', u'10.0', u'10.0', u'10.0', u'10.0', u'10.0', u'11.0', u'12.0', u'12.0', u'12.0', u'12.0', u'12.0', u'12.0', u'12.0', u'12.0', u'13.0', u'13.0', u'14.0', u'14.0', u'14.0', u'15.0', u'15.0', u'15.0', u'15.0', u'15.0', u'16.0', u'16.0', u'16.0', u'16.0', u'18.0', u'18.0', u'18.0', u'18.0', u'20.0', u'20.0', u'20.0', u'20.0', u'20.0', u'20.0', u'20.0', u'20.0', u'20.0', u'20.0', u'20.0', u'20.0', u'22.0', u'22.0', u'22.0', u'22.0', u'22.0', u'23.0', u'25.0', u'25.0', u'25.0', u'25.0', u'25.0', u'25.0', u'26.0', u'27.0', u'27.0', u'28.0', u'30.0', u'30.0', u'30.0', u'30.0', u'30.0', u'30.0', u'30.0', u'30.0', u'30.0', u'30.0', u'30.0', u'30.0', u'30.0', u'30.0', u'30.0', u'30.0', u'30.0', u'31.0', u'31.0', u'32.0', u'33.0', u'34.0', u'35.0', u'35.0', u'35.0', u'35.0', u'35.0', u'40.0', u'40.0', u'40.0', u'42.0', u'45.0']
[...]
 District of the oldest tree:  (u'5', u'1601')
[...]
 District of highest number of trees:  (u'16', 36)
[...]
21/12/08 15:06:50 INFO util.ShutdownHookManager: Shutdown hook called
21/12/08 15:06:50 INFO util.ShutdownHookManager: Deleting directory /tmp/spark-dc5493a4-34c3-48dc-8039-2bcdcee7ee9c
21/12/08 15:06:50 INFO util.ShutdownHookManager: Deleting directory /tmp/spark-4d40bbf4-1115-4a9e-b3cf-b7643679b605/pyspark-d168128e-9779-4f33-b7ab-5cfea98759b8
21/12/08 15:06:50 INFO util.ShutdownHookManager: Deleting directory /tmp/spark-4d40bbf4-1115-4a9e-b3cf-b7643679b605
```

### To notice
To retrieve easily our results, we can print a "Program response <question> <answer>" and grep it in a new file. For example, using the following command: `spark-submit --master=yarn other.py > test.txt | grep 'Program response'`.
