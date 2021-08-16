package com.atguigu.apitest.tableapi;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

public class TableTest3_FileOutput {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
        DataStreamSource<String> inputStream = env.readTextFile("D:\\ideaworkspace\\FlinkTutorial\\src\\main\\resources\\sensor");

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2、表的创建：连接外部系统，读取数据
        // 2.1 读取文件
        String inputFilePath = "D:\\ideaworkspace\\FlinkTutorial\\src\\main\\resources\\sensor";
        tableEnv.connect(new FileSystem().path(inputFilePath))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temp", DataTypes.DOUBLE()))
                .createTemporaryTable("inputTable");

        Table inputTable = tableEnv.from("inputTable");
//        inputTable.printSchema();
//        tableEnv.toAppendStream(inputTable, Row.class).print();

        // 3、查询转换
        // 3.1 Table API
        // 简单转换
        Table resultTable = inputTable.select("id, temp")
                .filter("id === 'sensor_6'");

        // 聚合统计
        Table aggTable = inputTable.groupBy("id")
                .select("id, id.count as count, temp.avg as avgTemp");

        // 3.2 SQL
        tableEnv.sqlQuery("select id, temp from inputTable where id = 'sensor_6'");
        Table sqlAggTable = tableEnv.sqlQuery("select id, count(id) as cnt, avg(temp) as avgTemp from inputTable group by id");

        // 4、输出到文件
        // 连接外部文件注册输出表
        String outputFilePath = "D:\\ideaworkspace\\FlinkTutorial\\src\\main\\resources\\out.txt";
        tableEnv.connect(new FileSystem().path(outputFilePath))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
//                        .field("cnt", DataTypes.BIGINT())
                        .field("temperature", DataTypes.DOUBLE()))
                .createTemporaryTable("outputTable");

        resultTable.insertInto("outputTable");
        // 输出到文件不支持带撤销的更新写入
//        aggTable.insertInto("outputTable");

        env.execute();
    }
}
