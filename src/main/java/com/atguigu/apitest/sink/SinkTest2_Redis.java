package com.atguigu.apitest.sink;

import com.atguigu.apitest.beans.SensorReading;
import com.atguigu.apitest.source.SourceTest4_UDF;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class SinkTest2_Redis {
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

        // 定义jedis连接配置
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost("localhost").setPort(6379)
                .build();

        dataStream.addSink(new RedisSink<SensorReading>(config, new MyRedisMapper()));

        // 执行
        env.execute();
    }

    // 自定义RedisMapper
    public static class MyRedisMapper implements RedisMapper<SensorReading> {
        // 定义保存数据到Redis的命令，存成hash表，hset sensor_temp id temperature
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "sensor_temp");
        }

        public String getKeyFromData(SensorReading sensorReading) {
            return sensorReading.getId();
        }

        public String getValueFromData(SensorReading sensorReading) {
            return sensorReading.getTemperature().toString();
        }
    }
}
