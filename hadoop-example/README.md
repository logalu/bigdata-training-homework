## Hadoop Map Reduce作业
统计每一个手机号耗费的总上行流量、下行流量、总流量。

### 基本思路：
Map 阶段：

读取一行数据，切分字段。
抽取手机号、上行流量、下行流量。
以手机号为 key，bean 对象为 value 输出，即 context.write(手机号，bean)。
Reduce 阶段：

累加上行流量和下行流量得到总流量。
实现自定义的 bean 来封装流量信息，并将 bean 作为 map 输出的 key 来传输。
MR 程序在处理数据的过程中会对数据排序 (map 输出的 kv 对传输到 reduce 之前，会排序)，排序的依据是 map 输出的 key
