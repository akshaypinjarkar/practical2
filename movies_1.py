from pyspark.sql.functions import min, max
from pyspark.sql.types import *
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("test").master("local[2]").getOrCreate()
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("genere", StringType(), True)
])
m1 = spark.read.option("header", "false").option("inferschema", "true")\
    .option("sep", "::").schema(schema).csv("C:\spark_file\\movies.dat")
m100= m1.withColumn("pincode", lit(None))
m100.show()
# spark.conf.set("spark.sql.repl.eagerEval.maxColumnWidth", 1000)
# m1.show(500, truncate=False)
# display(df, n=500, truncate = False)

m2 = m1.withColumn("year", regexp_extract("name","\((\d+)\)",1))
# m2.show()
m2.createOrReplaceTempView("bharti1")
# spark.sql("select id, name, genere from bharti1 ").show()
# m2.printSchema()

# m2.dropDuplicates(["year"]).show()
# m2.drop_duplicates(["year"]).show()
#.drop # m3= m2.withColumn("year", col("year").cast("int"))
# # m3.printSchema()
# #
# # m4 = m1.withColumn("genere1", split("genere", "\\|")[0]).withColumn("genere2", split("genere", "\\|")[1])\
# # .withColumn("genere3", split("genere", "\\|")[2])
# # m4.show()
# #
# # m5 = m3.select("id","name","year", explode(split("genere", "\\|")).alias("generes"))
# # m5.show()
# #
# # m6 = m5.groupBy("year").pivot("generes").count()
# # m6.show()
# #
# # m7 = m5.groupBy("generes").pivot("year").count()
# # m7.show()
# #
# # ### LIst of the oldet movie ###
# # schema = StructType([
# #     StructField("id", IntegerType(), True),
# #     StructField("name", StringType(), True),
# #     StructField("genere", StringType(), True)])
# # m1 = spark.read.option("header", "true").option("inferschema", "true")\
# #     .option("sep", "::").schema(schema).csv("C:\spark_file\\movies.dat")
# # m2 = m1.withColumn("year", regexp_extract("name","\((\d+)\)",1))
# # m3= m2.withColumn("year", col("year").cast("int"))
# # m8 = m3.agg(min("year")).first()[0]
# # m9 = m3.filter(col("year") == m8 )
# # m9.show()
# #
# # ### average_rating_per_movie ##
# # schema1 = StructType([StructField("uid", IntegerType(), True),StructField("mid", IntegerType(), True),
# #     StructField("rating", IntegerType(), True),StructField("ts", IntegerType(), True)])
# # r1 = spark.read.option("header","false").option("inferschema", "true")\
# #     .option("sep","::").schema(schema1).csv("D:\sandip1\Spark_RDD\Spark_RDD\data\\ratings_data\\ratings.dat")
# # r2 = r1.groupBy("mid").agg(avg("rating"))
# # r3 = r2.select("mid", round("avg(rating)", 2).alias("ratingss")).orderBy("mid")
# # r3.show()
# #
# #
# # ## movies each year ###
# # schema = StructType([
# #     StructField("id", IntegerType(), True),
# #     StructField("name", StringType(), True),
# #     StructField("genere", StringType(), True)])
# # m1 = spark.read.option("header", "true").option("inferschema", "true")\
# #     .option("sep", "::").schema(schema).csv("C:\spark_file\\movies.dat")
# # m2 = m1.withColumn("year", regexp_extract("name","\((\d+)\)",1))
# # m3 = m2.groupBy("year").count()
# # m3.show()
# #
# # ## movies_per_rating ###
# # schema1 = StructType([StructField("uid", IntegerType(), True),StructField("mid", IntegerType(), True),
# #     StructField("rating", IntegerType(), True),StructField("ts", IntegerType(), True)])
# # r1 = spark.read.option("header","false").option("inferschema", "true")\
# #     .option("sep","::").schema(schema1).csv("D:\sandip1\Spark_RDD\Spark_RDD\data\\ratings_data\\ratings.dat")
# # r2 = r1.groupBy("rating").count().orderBy("rating")
# # r2.show()
# #
# # #### num_users_per_movie ####
# # schema1 = StructType([StructField("uid", IntegerType(), True),StructField("mid", IntegerType(), True),
# #     StructField("rating", IntegerType(), True),StructField("ts", IntegerType(), True)])
# # r1 = spark.read.option("header","false").option("inferschema", "true")\
# #     .option("sep","::").schema(schema1).csv("D:\sandip1\Spark_RDD\Spark_RDD\data\\ratings_data\\ratings.dat")
# # r2 = r1.groupBy("mid").count().orderBy("mid")
# # r2.show()
# #
# # #### Total rating per movie ###
# # schema1 = StructType([StructField("uid", IntegerType(), True),StructField("mid", IntegerType(), True),
# #     StructField("rating", IntegerType(), True),StructField("ts", IntegerType(), True)])
# # r1 = spark.read.option("header","false").option("inferschema", "true")\
# #     .option("sep","::").schema(schema1).csv("D:\sandip1\Spark_RDD\Spark_RDD\data\\ratings_data\\ratings.dat")
# # r2 = r1.groupBy("mid").agg(sum("rating")).orderBy("mid")
# # r2.show()
#
# ### Distinct_Genres ####
# # schema = StructType([
# #     StructField("id", IntegerType(), True),
# #     StructField("name", StringType(), True),
# #     StructField("genere", StringType(), True)])
# # m1 = spark.read.option("header", "true").option("inferschema", "true")\
# #     .option("sep", "::").schema(schema).csv("C:\spark_file\\movies.dat")
# # m2 = m1.select("id", "name", explode(split("genere", "\\|")).alias("generes"))
# # m3 = m2.groupBy("generes").count()
# # m3.select("generes").show()
#
#
# # #### Latest_movies ###
# # schema = StructType([
# #     StructField("id", IntegerType(), True),
# #     StructField("name", StringType(), True),
# #     StructField("genere", StringType(), True)])
# # m1 = spark.read.option("header", "true").option("inferschema", "true")\
# #     .option("sep", "::").schema(schema).csv("C:\spark_file\\movies.dat")
# # m2 = m1.withColumn("year", regexp_extract("name", "\((\d+)\)",1))
# # m3 = m2.withColumn("year", col("year").cast("int"))
# # m4 = m3.agg(max("year")).first()[0]
# # m5 = m3.filter(col("year") == m4)
# # m5.show()
# #
# #
# # ### movies each genere ####
# # schema = StructType([
# #     StructField("id", IntegerType(), True),
# #     StructField("name", StringType(), True),
# #     StructField("genere", StringType(), True)])
# # m1 = spark.read.option("header", "true").option("inferschema", "true")\
# #     .option("sep", "::").schema(schema).csv("C:\spark_file\\movies.dat")
# # m2 = m1.select("id","name", explode(split("genere", "\\|")).alias("generes"))
# # m3 = m2.groupBy("generes").count().orderBy("generes")
# # m3.show()
# #
# #
# #
# # ### Movies_starting_with_Letters_or_Numbers ##
# # schema = StructType([
# #     StructField("id", IntegerType(), True),
# #     StructField("name", StringType(), True),
# #     StructField("genere", StringType(), True)])
# # m1 = spark.read.option("header", "true").option("inferschema", "true")\
# #     .option("sep", "::").schema(schema).csv("C:\spark_file\\movies.dat")
# # m2 = m1.withColumn("initial", substring("name", 1, 1))
# # m3 = m2.groupBy("initial").count().orderBy("initial")
# # m3.show()
# #
# # #### Top_10_Most_Viewed_Movies   ####
# # schema = StructType([
# #     StructField("id", IntegerType(), True),
# #     StructField("name", StringType(), True),
# #     StructField("genere", StringType(), True)])
# # m1 = spark.read.option("header", "true").option("inferschema", "true")\
# #     .option("sep", "::").schema(schema).csv("C:\spark_file\\movies.dat")
# # schema1 = StructType([StructField("uid", IntegerType(), True),StructField("mid", IntegerType(), True),
# #     StructField("rating", IntegerType(), True),StructField("ts", IntegerType(), True)])
# # r1 = spark.read.option("header","false").option("inferschema", "true")\
# #     .option("sep","::").schema(schema1).csv("D:\sandip1\Spark_RDD\Spark_RDD\data\\ratings_data\\ratings.dat")
# # r2 = r1.groupBy("mid").count()
# # join = m1.join(r2, m1.id == r2.mid, "inner")
# # join.select("id","name","count").orderBy(col("count").desc()).show(10)
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
