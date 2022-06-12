# 题目一
## 使用 RDD API 实现带词频的倒排索引
### 实现类 geebang.spark.homework.RevertedIndex
### 思路
* 先将{字符_文件名}作为key，统计单文件中字符同一个字符出现次数
* 再将{字符}作为key, 将文件名和出现次数拼接起来，reduceByKey之后，重新将文件名和出现次数map成Array




# 题目2
## Distcp 的 Spark 实现
### 实现类 geebang.spark.homework.SparkDiskCP
### 思路
* 先将source文件和targe目录组装成RDD
* 通过mapPartitions对文件进行拷贝

