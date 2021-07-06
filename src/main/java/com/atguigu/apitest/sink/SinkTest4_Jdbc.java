package com.atguigu.apitest.sink;

import com.atguigu.apitest.beans.SensorReading;
import com.atguigu.apitest.source.SourceTest4_UDF;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class SinkTest4_Jdbc {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        // 从文件读取数据
//        DataStream<String> inputStream = env.readTextFile("D:\\ideaworkspace\\FlinkTutorial\\src\\main\\resources\\sensor");
//
//        // 转换成SensorReading类型
//        DataStream<SensorReading> dataStream = inputStream.map(new MapFunction<String, SensorReading>() {
//            public SensorReading map(String s) throws Exception {
//                String[] fields = s.split(",");
//                return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
//            }
//        });

        DataStreamSource<SensorReading> dataStream = env.addSource(new SourceTest4_UDF.MySensorSource());
        dataStream.addSink(new MyJdbcSink());

        env.execute();
    }

    // 实现自定义的SinkFunction
    public static class MyJdbcSink extends RichSinkFunction<SensorReading> {
        // 声明连接和预编译语句
        Connection connection = null;
        PreparedStatement insertStatement = null;
        PreparedStatement updateStatement = null;
        public void invoke(SensorReading value, Context context) throws Exception {
            // 每来一条数据，调用连接，执行sql
            // 执行更新语句，没有更新就插入
            updateStatement.setDouble(1, value.getTemperature());
            updateStatement.setString(2, value.getId());
            updateStatement.execute();
            if (updateStatement.getUpdateCount() == 0) {
                insertStatement.setString(1, value.getId());
                insertStatement.setDouble(2, value.getTemperature());
                insertStatement.execute();
            }
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/routing", "root", "root");
            insertStatement = connection.prepareStatement("insert into sensor_temp(id,temp) values (?,?)");
            updateStatement = connection.prepareStatement("update sensor_temp set temp=? where id=?");
        }

        @Override
        public void close() throws Exception {
            insertStatement.close();
            updateStatement.close();
            connection.close();
        }
    }
}
