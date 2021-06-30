package com.atguigu.apitest.sink;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

import java.util.Properties;

public class SinkTest1_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
//        DataStream<String> inputStream = env.readTextFile("D:\\ideaworkspace\\FlinkTutorial\\src\\main\\resources\\sensor");

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        // 从文件读取数据
        DataStream<String> inputStream = env.addSource(new FlinkKafkaConsumer010<String>("test",
                new SimpleStringSchema(), properties));

        // 转换成SensorReading类型
        DataStream<String> dataStream = inputStream.map(new MapFunction<String, String>() {
            public String map(String s) throws Exception {
                String[] fields = s.split(",");
                return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2])).toString();
            }
        });

        dataStream.addSink(new FlinkKafkaProducer010<String>("localhost:9092", "linlin", new SimpleStringSchema()));

        env.execute();
    }
}
