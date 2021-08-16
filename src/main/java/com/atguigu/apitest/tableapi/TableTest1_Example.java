package com.atguigu.apitest.tableapi;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class TableTest1_Example {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
        DataStreamSource<String> inputStream = env.readTextFile("D:\\ideaworkspace\\FlinkTutorial\\src\\main\\resources\\sensor");

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map((s) -> {
            String[] fields = s.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 3、创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 4、基于流创建一张表
        Table dataTable = tableEnv.fromDataStream(dataStream);
        // 5、调用tableAPI进行转换操作
        Table resultTable = dataTable.select("id, temperature")
                .where("id='sensor_1'");
        // 6、执行SQL
        tableEnv.createTemporaryView("sensor", dataTable);
        String sql = "select id,temperature from sensor where id='sensor_1'";
        Table resultSqlTable = tableEnv.sqlQuery(sql);

        tableEnv.toAppendStream(resultTable, Row.class).print("result");
        tableEnv.toAppendStream(resultSqlTable, Row.class).print("sql");

        env.execute();
    }
}
