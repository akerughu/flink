package com.atguigu.apitest.tableapi;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

public class TableTest4_KafkaPipeLine {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2、连接Kafka读取数据
        tableEnv.connect(new Kafka()
                .version("0.10")
                .topic("sensor")
                .property("zookeeper.connect", "localhost:2181")
                .property("bootstrap.servers", "localhost:9092")
        ).withFormat(new Csv()).withSchema(new Schema()
                .field("id", DataTypes.STRING())
                .field("timestamp", DataTypes.BIGINT())
                .field("temp", DataTypes.DOUBLE())
        ).createTemporaryTable("inputTable");

        // 3、查询转换
        Table sensorTable = tableEnv.from("inputTable");
        // 简单转换
        Table resultTable = sensorTable.select("id, temp")
                .filter("id === 'sensor_6'");

        // 聚合统计
        Table aggTable = sensorTable.groupBy("id")
                .select("id, id.count as count, temp.avg as avgTemp");

        // 4、建立Kafka连接输出到不同的topic下
        tableEnv.connect(new Kafka()
                .version("0.10")
                .topic("sinktest")
                .property("zookeeper.connect", "localhost:2181")
                .property("bootstrap.servers", "localhost:9092"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
//                        .field("cnt", DataTypes.BIGINT())
                        .field("temp", DataTypes.DOUBLE()))
                .createTemporaryTable("outputTable");

        resultTable.insertInto("outputTable");

        resultTable.insertInto("outputTable");

        env.execute();
    }
}
