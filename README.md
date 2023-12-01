# Spark Optimization Project

The aim of this project was to improve performance of spark job.

### Overview of Spark job
- The spark job was reading data from 2 parquet files answers and questions
- The goal was to find the count of answers to questions by month

### Original Spartk job
- The questions dataframe has ~86K rows and the answers dataframe has ~70K rows
- The original spark job was grouping the answers dataframe and adding a new column to get month from creation_date
- It then aggregated the data using groupBy question_id and month to get the count of answers by month
- The questions dataframe was joined to answers dataframe on question_id

### Problems with the original spark job
- The groupby caused data shuffling across default 200 partitions
- The join also caused data shuffling to merge data on executor nodes by question_id. Shuffling moves the data across nodes if the join key is not present on the current executor node

### Optimization
- Reduced the default shuffle partitions from 200 to 50 as the number of records in each partition was very small and it increased the time taken to execute groupBy query
- Added a broadcast join for answers dataframe so that each executor has a copy of the dataframe thus reducing the shuffling during join
