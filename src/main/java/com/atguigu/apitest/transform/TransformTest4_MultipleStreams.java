package com.atguigu.apitest.transform;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Collections;

public class TransformTest4_MultipleStreams {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
        DataStream<String> inputStream = env.readTextFile("D:\\ideaworkspace\\FlinkTutorial\\src\\main\\resources\\sensor");

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(new MapFunction<String, SensorReading>() {
            public SensorReading map(String s) throws Exception {
                String[] fields = s.split(",");
                return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
            }
        });

        //1、分流。按照温度值30°C为界分为两条流
        SplitStream<SensorReading> splitStream = dataStream.split(new OutputSelector<SensorReading>() {
            public Iterable<String> select(SensorReading value) {
                return (value.getTemperature() > 30d) ? Collections.singletonList("high") : Collections.singletonList("low");
            }
        });

        DataStream<SensorReading> highTempStream = splitStream.select("high");
        DataStream<SensorReading> lowTempStream = splitStream.select("low");
        DataStream<SensorReading> allTempStream = splitStream.select("high", "low");

        highTempStream.print("high");
//        lowTempStream.print("low");
//        allTempStream.print("all");

        // 2、将高温流转换成二元组类型，与低温流连接合并之后，输出状态信息
        DataStream<Tuple2<String, Double>> warningStream = highTempStream.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
            public Tuple2<String, Double> map(SensorReading sensorReading) throws Exception {
                return new Tuple2<String, Double>(sensorReading.getId(), sensorReading.getTemperature());
            }
        });

        ConnectedStreams<Tuple2<String, Double>, SensorReading> connectedStreams = warningStream.connect(lowTempStream);
        SingleOutputStreamOperator<Object> resultStream = connectedStreams.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {
            public Object map1(Tuple2<String, Double> value) throws Exception {
                return new Tuple3<String, Double, String>(value.f0, value.f1, "high temp warning");
            }

            public Object map2(SensorReading value) throws Exception {
                return new Tuple2<String, String>(value.getId(), "normal");
            }
        });

        resultStream.print();

        env.execute();
    }
}
