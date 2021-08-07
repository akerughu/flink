package com.atguigu.apitest.state;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StateTest4_FaultTolerance {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1、状态后端配置
        env.setStateBackend(new MemoryStateBackend());
        env.setStateBackend(new FsStateBackend(""));
        env.setStateBackend(new RocksDBStateBackend(""));

        // 2、检查点配置
        // checkpoint保存的时间间隔
        env.enableCheckpointing(300);

        // 高级选项
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // checkpoint处理的最大时间
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        // 同时并行的checkpoint数量
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        // 前一次checkpoint结束到下一次checkpoint开始的时间间隔不能小于100毫秒
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(100L);
        // 更倾向于用检查点恢复还是最近的保存点恢复数据，默认false
        env.getCheckpointConfig().setPreferCheckpointForRecovery(true);
        // 允许容忍checkpoint失败多少次，默认0
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(0);

        // 3、重启策略配置
        // 固定延迟重启
        // 每隔10秒钟尝试重启，最多三次
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000L));
        // 失败率重启
        // 10分钟内最多重启三次，每隔1分钟重启1次
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.minutes(10), Time.minutes(1)));

        // socket文本流
        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 7777);

        // 转换成SensorReading类型，分配时间戳和watermark
        DataStream<SensorReading> dataStream = inputStream.map(new MapFunction<String, SensorReading>() {
            public SensorReading map(String s) throws Exception {
                String[] fields = s.split(",");
                return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
            }
        });

        env.execute();
    }
}
