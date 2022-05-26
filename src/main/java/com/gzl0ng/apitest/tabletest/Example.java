package com.gzl0ng.apitest.tabletest;

import com.gzl0ng.com.gzl0ng.apitest.beans.SensoReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.net.URL;

/**
 * @author 郭正龙
 * @date 2021-12-04
 */
public class Example {

    public static void main(String[] args) throws Exception {
        //创建流执行环境，读取数据并转换为样例类POJO
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        URL resource = Example.class.getResource("/sensor.txt");
        DataStreamSource<String> inputStream = env.readTextFile(resource.getPath());

        DataStream<SensoReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensoReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        //1.基于env创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2..基于tableEnv，将流转换成表
        Table table = tableEnv.fromDataStream(dataStream);

        //3.转换操作，得到提取结果
        //3.1 调用tableAPI，做转换操作
        Table resultTable = table
                .select("id,temperature")
                .filter("id == 'sensor_1'");
        //3.2写sql实现转换
        tableEnv.createTemporaryView("sensor",dataStream);
        Table sqlTable = tableEnv.sqlQuery("select id,temperature from sensor where id = 'sensor_1'");

        //4.将表转换成流
        DataStream<Row> resultStream = tableEnv.toAppendStream(resultTable, Row.class);
        DataStream<Row> sqlResultStream = tableEnv.toAppendStream(sqlTable, Row.class);
//        resultStream.print("result);
        sqlResultStream.print("sqlResult");

        env.execute();
    }
}
