package com.atguigu.apitest.window;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowTest2_CountWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
//        DataStream<String> inputStream = env.readTextFile("D:\\ideaworkspace\\FlinkTutorial\\src\\main\\resources\\sensor");

        // socket文本流
        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 7777);

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(new MapFunction<String, SensorReading>() {
            public SensorReading map(String s) throws Exception {
                String[] fields = s.split(",");
                return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
            }
        });

        // 开计数窗口测试
        DataStream<Double> avgTempResultStream = dataStream.keyBy("id")
                .countWindow(10, 2)
                .aggregate(new MyAvgTemp());

        avgTempResultStream.print();

        // 执行
        env.execute();
    }

    public static class MyAvgTemp implements AggregateFunction<SensorReading, Tuple2<Double, Integer>, Double> {
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<Double, Integer>(0.0, 0);
        }

        public Tuple2<Double, Integer> add(SensorReading value, Tuple2<Double, Integer> accumulator) {
            return new Tuple2<Double, Integer>(accumulator.f0 + value.getTemperature(), accumulator.f1 + 1);
        }

        public Double getResult(Tuple2<Double, Integer> accumulator) {
            return accumulator.f0 / accumulator.f1;
        }

        public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> doubleIntegerTuple2, Tuple2<Double, Integer> acc1) {
            return new Tuple2<Double, Integer>(doubleIntegerTuple2.f0 + acc1.f0, doubleIntegerTuple2.f1 + acc1.f1);
        }
    }
}
