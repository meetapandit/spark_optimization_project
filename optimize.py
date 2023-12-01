'''
Optimize the query plan

Suppose we want to compose query in which we get for each question also the number of answers to this question for each month. See the query below which does that in a suboptimal way and try to rewrite it to achieve a more optimal plan.
'''


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, month
import time

import os


spark = SparkSession.builder.appName('Optimize I').getOrCreate()

base_path = '/Users/meetapandit/DE_Bootcamp/spark_optimization_project/Optimization/data'
print("base_path", base_path)
project_path = ('/').join(base_path.split('/')[0:-3]) 

answers_input_path = '/Users/meetapandit/DE_Bootcamp/spark_optimization_project/Optimization/data/answers'

questions_input_path = '/Users/meetapandit/DE_Bootcamp/spark_optimization_project/Optimization/data/questions'

answersDF = spark.read.parquet(answers_input_path)
answersDF.show()

questionsDF = spark.read.parquet(questions_input_path)
questionsDF.show()

'''
Answers aggregation

Here we : get number of answers per question per month
'''
start_time = time.time()
answers_month = answersDF.withColumn('month', month('creation_date')).groupBy('question_id', 'month').agg(count('*').alias('cnt'))

print("groupby time", time.time() - start_time)

start_time_join = time.time()
resultDF = questionsDF.join(answers_month, 'question_id').select('question_id', 'creation_date', 'title', 'month', 'cnt')

print("join time", time.time() - start_time_join)

resultDF.orderBy('question_id', 'month').show()

'''
Task:

see the query plan of the previous result and rewrite the query to optimize it
'''
resultDF.explain(mode="formatted")