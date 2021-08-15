package com.atguigu.apitest.tableapi;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class TableTest2_CommonApi {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
        DataStreamSource<String> inputStream = env.readTextFile("D:\\ideaworkspace\\FlinkTutorial\\src\\main\\resources\\sensor");

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] fields = s.split(",");
                return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
            }
        });

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1.1 基于老版本planner的流处理
        EnvironmentSettings oldStreamSettings = EnvironmentSettings.newInstance()
                .useOldPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment oldStreamTableEnvironment = StreamTableEnvironment.create(env, oldStreamSettings);

        // 1.2 基于老版本planner的批处理
        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment oldBatchTableEnv = BatchTableEnvironment.create(batchEnv);

        // 1.3 基于Blink的流处理
        EnvironmentSettings blinkStreamSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment blinkStreamTableEnvironment = StreamTableEnvironment.create(env, blinkStreamSettings);
        // 1.4 基于Blink的批处理
        EnvironmentSettings blinkBatchSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inBatchMode()
                .build();

        TableEnvironment blinkBatchTableEnv = TableEnvironment.create(blinkBatchSettings);
    }
}
