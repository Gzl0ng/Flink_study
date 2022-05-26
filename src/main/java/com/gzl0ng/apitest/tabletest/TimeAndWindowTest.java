package com.gzl0ng.apitest.tabletest;

import com.gzl0ng.com.gzl0ng.apitest.beans.SensoReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.GroupWindow;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.plan.logical.TumblingGroupWindow;
import org.apache.flink.types.Row;

/**
 * @author 郭正龙
 * @date 2021-12-05
 */
public class TimeAndWindowTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //设置时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //1.创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStream<String> inputStream = env.readTextFile("E:\\IntelliJ_work\\Flinktutorial\\src\\main\\resources\\sensor.txt");

        DataStream<SensoReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensoReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<SensoReading>() {
            @Override
            public long extractAscendingTimestamp(SensoReading element) {
                return element.getTimestamp() * 1000L;
            }
        });
//        dataStream.print();
        //流的开窗
//        dataStream.keyBy("id")
//                .timeWindow()

        //将流转换成表,直接定义时间字段(这里使用每条事件完成的本地时间作为protime，上面全局定义的eventime时间语义就无效了)
        Table sensorTable = tableEnv.fromDataStream(dataStream, "id,temperature,timestamp,pt.proctime");
        //这里使用之前指定的时间戳作为事件时间
        Table sensorTable2 = tableEnv.fromDataStream(dataStream, "id,temperature,rt.rowtime");

        //1 Table API实现开窗
        //1.1 Group Window聚合操作
        Table resultTable = sensorTable2
                .window(Tumble.over("10.seconds").on("rt").as("tw"))
                .groupBy("id,tw")
                .select("id,id.count,tw.end");
        //1.2 Over Window聚合操作
        Table overResultTable = sensorTable2
                .window(Over.partitionBy("id").orderBy("rt").preceding("2.rows").as("ow"))
                .select("id,rt,id.count over ow,temperature.avg over ow");

        //2.SQL实现开窗
        //2.1 Group Window
        tableEnv.createTemporaryView("sensor",sensorTable2);
        Table resultSqlTable = tableEnv.sqlQuery(
                "select id,count(id),hop_end(rt,interval '4' second,interval '10' second)" +
                        " from sensor" +
                        " group by id,hop(rt,interval '4' second,interval '10' second)"
        );
        //2.2Over Window
        Table resultOverSqlTable = tableEnv.sqlQuery(
                "select id,rt,count(id) over w,avg(temperature) over w" +
                        " from sensor" +
                        " window w as (" +
                        "partition by id order by rt rows between 2 preceding and current row)"
        );

        //表头输出
//        sensorTable.printSchema();
        sensorTable2.printSchema();

        //数据输出
//        tableEnv.toAppendStream(sensorTable, Row.class).print();
//        tableEnv.toAppendStream(sensorTable2,Row.class).print();
        //Table API开窗后输出
//        tableEnv.toAppendStream(resultTable,Row.class).print();
//        tableEnv.toAppendStream(overResultTable,Row.class).print();
        //SQL开窗后输出
//        tableEnv.toAppendStream(resultSqlTable,Row.class).print();
        tableEnv.toAppendStream(resultOverSqlTable,Row.class).print();

        env.execute("time and window test job");

    }
}
