package com.atguigu.apitest.source;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.HashMap;
import java.util.Properties;
import java.util.Random;

public class SourceTest4_UDF {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", "localhost:9092");
//        properties.setProperty("group.id", "consumer-group");
//        properties.setProperty("key.deserializer",
//                "org.apache.kafka.common.serialization.StringDeserializer");
//        properties.setProperty("value.deserializer",
//                "org.apache.kafka.common.serialization.StringDeserializer");
//        properties.setProperty("auto.offset.reset", "latest");

        // 从文件读取数据
        DataStream<SensorReading> dataStream = env.addSource(new MySensorSource());

        // 打印输出
        dataStream.print();

        // 执行
        env.execute();
    }

    // 实现自定义的SourceFunction
    public static class MySensorSource implements SourceFunction<SensorReading> {

        // 定义一个标识位，用来控制数据的产生
        private boolean running = true;
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            // 定义一个随机数发生器
            Random random = new Random();
            // 设置十个传感器的初始温度
            HashMap<String, Double> sensorTempMap = new HashMap<String, Double>();
            for (int i=0; i<10; i++) {
                sensorTempMap.put("sensor_" + (i+1), 60 + random.nextGaussian() * 20);
            }
            while (running) {
                for (String sensorId:sensorTempMap.keySet()) {
                    // 在当前温度基础上做随机波动
                    Double newTemp = sensorTempMap.get(sensorId) + random.nextGaussian();
                    sensorTempMap.put(sensorId, newTemp);
                    ctx.collect(new SensorReading(sensorId, System.currentTimeMillis(), newTemp));
                }
                // 控制输出频率
                Thread.sleep(1000L);
            }
        }

        public void cancel() {
            running = false;
        }
    }
}
