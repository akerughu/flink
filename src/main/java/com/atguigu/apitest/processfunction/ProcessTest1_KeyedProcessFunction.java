package com.atguigu.apitest.processfunction;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class ProcessTest1_KeyedProcessFunction {
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

        // 测试KeyedProcessFunction，先分组，然后自定义处理
        dataStream.keyBy("id")
                .process(new MyProcess())
                .print();

        env.execute();
    }

    // 实现自定义的处理函数
    public static class MyProcess extends KeyedProcessFunction<Tuple, SensorReading, Integer> {
        ValueState<Long> tsTimerState;
        @Override
        public void open(Configuration parameters) throws Exception {
            tsTimerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-timer", Long.class));
        }

        public void processElement(SensorReading sensorReading, Context context, Collector<Integer> collector) throws Exception {
            collector.collect(sensorReading.getId().length());

            // context
            context.getCurrentKey();
            context.timestamp();
            context.timerService().currentProcessingTime();
            context.timerService().currentWatermark();
//            context.timerService().registerEventTimeTimer((sensorReading.getTimestamp() + 10) * 1000L);
            context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime() + 5000L);
            tsTimerState.update(context.timerService().currentProcessingTime() + 5000L);
//            context.timerService().deleteEventTimeTimer();
//            context.timerService().deleteProcessingTimeTimer(tsTimerState.value());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Integer> out) throws Exception {
            System.out.println(timestamp + "定时器触发");
            ctx.getCurrentKey();
//            ctx.output();
            ctx.timeDomain();
        }

        @Override
        public void close() throws Exception {
            tsTimerState.clear();
        }
    }
}
