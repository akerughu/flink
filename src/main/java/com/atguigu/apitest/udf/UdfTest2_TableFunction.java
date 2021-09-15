package com.atguigu.apitest.udf;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

public class UdfTest2_TableFunction {
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
        });

        // 将流转换成表
        Table sensorTable = tableEnv.fromDataStream(dataStream, "id, timestamp as ts, temperature as temp");

        // 自定义表函数 实现将id拆分并输出
        // table API
        Split split = new Split("_");

        // 需要在环境中注册UDF
        tableEnv.registerFunction("split", split);
        Table resultTable = sensorTable
                .joinLateral("split(id) as (word,length)")
                .select("id, ts, word, length");

        // Flink SQL
        tableEnv.createTemporaryView("sensor", sensorTable);
        Table resultSqlTable = tableEnv.sqlQuery("select id,ts,word,length from sensor,lateral table" +
                "(split(id)) as splitid(word,length)");

        // 打印输出
        tableEnv.toAppendStream(resultTable, Row.class).print("result");
        tableEnv.toAppendStream(resultSqlTable, Row.class).print("sql");

        env.execute();
    }
    // 实现自定义tablefunction
    public static class Split extends TableFunction<Tuple2<String, Integer>> {
        // 定义属性分隔符
        private String separator = ",";

        public Split(String separator) {
            this.separator = separator;
        }

        // 必须实现一个eval（）方法，没有返回值
        public void eval(String str) {
            for (String s : str.split(separator)) {
                collect(new Tuple2<>(s, s.length()));
            }
        }
    }
}
