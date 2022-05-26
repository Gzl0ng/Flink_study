package com.gzl0ng.apitest.sink;

import com.gzl0ng.apitest.source.SourceTest4_UDF;
import com.gzl0ng.com.gzl0ng.apitest.beans.SensoReading;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author 郭正龙
 * @date 2021-11-03
 * 自定义实现一个sink，向mysql写入数据
 */
public class SinkTest4_jdbc {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        DataStream<String> inputStream = env.readTextFile("E:\\IntelliJ_work\\Flinktutorial\\src\\main\\resources\\sensor.txt");
//
//        DataStream<SensoReading> dataStream = inputStream.map(line -> {
//            String[] fields = line.split(",");
//            return new SensoReading(fields[0], new Long(fields[1]), new Double(fields[2]));
//        });

        DataStream<SensoReading> dataStream = env.addSource(new SourceTest4_UDF.MySensorSource());



        dataStream.addSink(new MyJdbcSink());

        env.execute();
    }

    //自定义的SinkFunction
    public static class MyJdbcSink extends RichSinkFunction<SensoReading> {
        Connection connection = null;

        PreparedStatement insertStmt = null;
        PreparedStatement updataStmt = null;
        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test","root","123456");
            insertStmt = connection.prepareStatement("insert into sensor_temp(id,temp) values(?,?)");
            updataStmt = connection.prepareStatement("update sensor_temp set temp = ? where id = ?");
        }

        //每来一条数据，调用连接，执行sql
        @Override
        public void invoke(SensoReading value, Context context) throws Exception {
            //直接执行更新语句，如果没有更新那么就插入
            updataStmt.setDouble(1,value.getTemperature());
            updataStmt.setString(2,value.getId());
            updataStmt.execute();
            if (updataStmt.getUpdateCount() == 0){
                insertStmt.setString(1,value.getId());
                insertStmt.setDouble(2,value.getTemperature());
                insertStmt.execute();
            }
        }

        @Override
        public void close() throws Exception {
            insertStmt.close();
            updataStmt.close();
            connection.close();
        }
    }
}
