# Spark Structured Streaming Examples
Spark structured streaming examples with using of version 2.4.0

# Support matrix for joins in streaming queries

| Left Input | Right Input  | Join Type   | Implementation |
| ---------- | ------------ | ----------- | -------------- |
| Static     | Static       | All types   | TBD |
| Stream     | Static       | Inner       | TBD |
|            |              | Left Outer  | TBD |
|            |              | Right Outer | Not supported |
|            |              | Full Outer  | Not supported |
| Static     | Stream       | Inner       | TBD |
|            |              | Left Outer  | Not supported |
|            |              | Right Outer | TBD |
|            |              | Full Outer  | Not supported |
| Stream     | Stream       | Inner       | ..streamstream.InnerJoinApp, ..streamstream.InnerJoinWithWatermarkingApp |
|            |              | Left Outer  | ..streamstream.LeftOuterJoinWithWatermarkingApp |
|            |              | Right Outer | TBD |
|            |              | Full Outer  | Not supported |
** Base package: com.phylosoft.spark.learning.sql.streaming.operations.join

# Use cases of processing modes (Triggers modes)
1) Default;
2) Fixed interval micro-batches;
3) One-time micro-batch;
2) Continuous with fixed checkpoint interval;

# Optimizations
1) Tungsten execution engine;
2) Catalyst query optimizer;
3) Cost-based optimizer;

# Structured Sessionization
1) KeyValueGroupedDataset.mapGroupsWithState;
2) KeyValueGroupedDataset.flatMapGroupsWithState;

# Links
1) [Structured Streaming Programming Guide](http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html);
2) [Stream-Stream Joins using Structured Streaming (Scala)](https://docs.databricks.com/spark/latest/structured-streaming/examples.html#stream-stream-joins-scala);
3) [Easy, Scalable, Fault-Tolerant Stream Processing with Structured Streaming in Apache Spark](https://databricks.com/session/easy-scalable-fault-tolerant-stream-processing-with-structured-streaming-in-apache-spark);
4) [Easy, Scalable, Fault-Tolerant Stream Processing with Structured Streaming in Apache Spark - continues](https://databricks.com/session/easy-scalable-fault-tolerant-stream-processing-with-structured-streaming-in-apache-spark-continues);
5) [Deep Dive into Stateful Stream Processing in Structured Streaming](https://databricks.com/session/deep-dive-into-stateful-stream-processing-in-structured-streaming);
6) [Monitoring Structured Streaming Applications Using Web UI](https://databricks.com/session/monitoring-structured-streaming-applications-using-web-ui);
