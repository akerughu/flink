package com.atguigu.apitest.tableapi;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class TableTest5_TimeAndWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 2、读入文件数据，得到一个数据流
        // 从文件读取数据
        DataStream<String> inputStream = env.readTextFile("D:\\ideaworkspace\\FlinkTutorial\\src\\main\\resources\\sensor");

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map((s) -> {
            String[] fields = s.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(SensorReading sensorReading) {
                        return sensorReading.getTimestamp()*1000;
                    }
                });

        // 4、将流转换成表，定义时间特性
//        Table dataTable = tableEnv.fromDataStream(dataStream, "id, timestamp as ts, temperature as temp, pt.proctime");
        Table dataTable = tableEnv.fromDataStream(dataStream, "id, timestamp as ts, temperature as temp, rt.rowtime");
        tableEnv.createTemporaryView("sensor", dataTable);

        // 5、窗口操作
        // Group Windows
        // table api
        Table resultTable = dataTable.window(Tumble.over("10.seconds")
                .on("rt")
                .as("tw"))
                .groupBy("id, tw")
                .select("id, id.count, temp.avg, tw.end");
        // SQL
        Table resultSqlTable = tableEnv.sqlQuery("select id,count(id) as cnt,avg(temp) as avgTemp,TUMBLE_END(rt,interval '10' second) " +
                "from sensor group by id,TUMBLE(rt,interval '10' second)");

//        dataTable.printSchema();
//        tableEnv.toAppendStream(dataTable, Row.class).print();
        tableEnv.toRetractStream(resultTable, Row.class).print("result");
//        tableEnv.toRetractStream(resultSqlTable, Row.class).print("sql");

        env.execute();
    }
}
