package com.gzl0ng.apitest.tableapi;

import com.gzl0ng.com.gzl0ng.apitest.beans.SensoReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author 郭正龙
 * @date 2021-11-09
 */
public class TableTest5_TimeAndWindow {
    public static void main(String[] args) throws Exception {
        //1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.读入文件数据转换为流
        DataStream<String> inputStream = env.readTextFile("E:\\IntelliJ_work\\Flinktutorial\\src\\main\\resources\\sensor.txt");

        //3.转换成POJO
        DataStream<SensoReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensoReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensoReading>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(SensoReading element) {
                return element.getTimestamp() * 1000L;
            }
        });

        //4.将流转换为表,定义时间特性(增加一个字段，完成时间和事件时间,pt.proctime,rt.rowtime)
//        Table dataTable = tableEnv.fromDataStream(dataStream, "id,timestamp as ts,temperature as temp,pt.proctime");
        Table dataTable = tableEnv.fromDataStream(dataStream, "id,timestamp as ts,temperature as temp,rt.rowtime");

        tableEnv.createTemporaryView("sensor",dataTable);

        //5.窗口操作
        //5.1 Group Window
        //table API
        Table resultTable = dataTable.window(Tumble.over("10.seconds").on("rt").as("tw"))//10秒钟事件语义滚动窗口
                .groupBy("id,tw")
                .select("id,id.count,temp.avg,tw.end");
        //SQL
        Table resultSqlTable = tableEnv.sqlQuery("select id,count(id) as cnt,avg(temp) as avgTemp,tumble_end(rt,interval '10' second) " +
                "from sensor group by id,tumble(rt,interval '10' second)");

        //5.2 Over Window
        //table API
        Table overResult = dataTable.window(Over.partitionBy("id").orderBy("rt").preceding("2.rows").as("ow"))
                .select("id,rt,id.count over ow,temp.avg over ow");
        //SQL
        Table overSqlResult = tableEnv.sqlQuery("select id,rt,count(id) over ow,avg(temp) over ow" +
                " from sensor" +
                " window ow as (partition by id order by rt rows between 2 preceding and current row)");

//        dataTable.printSchema();
//        tableEnv.toAppendStream(dataTable, Row.class).print();
//        tableEnv.toAppendStream(resultTable, Row.class).print("result");
//        tableEnv.toRetractStream(resultSqlTable, Row.class).print("sql");
        tableEnv.toAppendStream(overResult, Row.class).print("result");
        tableEnv.toRetractStream(overSqlResult, Row.class).print("sql");



        env.execute();
    }
}
