package com.gzl0ng.apitest.tabletest.udftest;

import com.gzl0ng.com.gzl0ng.apitest.beans.SensoReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @author 郭正龙
 * @date 2021-12-06
 */
//自定义表函数
public class TableFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStream<String> inputStream = env.readTextFile("E:\\IntelliJ_work\\Flinktutorial\\src\\main\\resources\\sensor.txt");
        DataStream<SensoReading> dataStream = inputStream.map(line -> {
            String[] felds = line.split(",");
            return new SensoReading(felds[0], new Long(felds[1]), new Double(felds[2]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<SensoReading>() {
            @Override
            public long extractAscendingTimestamp(SensoReading element) {
                return element.getTimestamp();
            }
        });

        //将流转换成表，直接定义时间字段
        Table sensorTable = tableEnv.fromDataStream(dataStream, "id,temperature,timestamp.rowtime as ts");
//        sensorTable.printSchema();

        //先创建一个UDF对象
        Split split = new Split("_");
        tableEnv.registerFunction("split",split);

        //TableAPI调用
        Table resultTable = sensorTable.joinLateral("split(id) as (word,length)")
                .select("id,ts,word,length");
//        sensorTable.leftOuterJoinLateral()

        //SQL调用
        tableEnv.createTemporaryView("sensor",sensorTable);
        Table sqlResultTable = tableEnv.sqlQuery("select id,ts,word,length from sensor,lateral table(split(id)) as splitid(word,length)");

        //输出
//        tableEnv.toAppendStream(resultTable, Row.class).print();
        tableEnv.toAppendStream(sqlResultTable,Row.class).print();

        env.execute();
    }


    //自定义TableFunction，实现分隔字符串并统计长度(word,length)
    public static class Split extends TableFunction<Tuple2<String,Integer>>{
        private String separator;

        public Split(String separator) {
            this.separator = separator;
        }

        public void eval(String str){
            for (String s : str.split(separator)) {
                collector.collect(new Tuple2<>(s,s.length()));
            }
        }
    }
}
