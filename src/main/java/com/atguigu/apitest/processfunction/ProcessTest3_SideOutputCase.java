package com.atguigu.apitest.processfunction;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class ProcessTest3_SideOutputCase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // socket文本流
        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 7777);

        // 转换成SensorReading类型，分配时间戳和watermark
        DataStream<SensorReading> dataStream = inputStream.map(new MapFunction<String, SensorReading>() {
            public SensorReading map(String s) throws Exception {
                String[] fields = s.split(",");
                return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
            }
        });

        // 定义一个OutputTag，用来表示侧输出流
        final OutputTag<SensorReading> lowTempTag = new OutputTag<SensorReading>("lowTemp") {
        };

        // 测试ProcessFunction，自定义侧输出流实现分流操作
        SingleOutputStreamOperator<SensorReading> highTempStream = dataStream.process(new ProcessFunction<SensorReading, SensorReading>() {
            public void processElement(SensorReading sensorReading, Context context, Collector<SensorReading> collector) throws Exception {
                // 判断温度大于30度，高温流输出到主流；小于30度，低温流输出到侧输出流
                if (sensorReading.getTemperature() > 30) {
                    collector.collect(sensorReading);
                } else {
                    context.output(lowTempTag, sensorReading);
                }
            }
        });
        highTempStream.print("high-temp");
        highTempStream.getSideOutput(lowTempTag).print("low-temp");
        env.execute();
    }
}
