package com.gzl0ng.apitest.tabletest.udftest;

import com.gzl0ng.com.gzl0ng.apitest.beans.SensoReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

/**
 * @author 郭正龙
 * @date 2021-12-06
 */
public class ScalarFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

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

        Table sensorTable = tableEnv.fromDataStream(dataStream,"id,temperature,timestamp.rowtime as ts");

        //注册自定义函数
        HashCode hashCode = new HashCode(1.23);
        tableEnv.registerFunction("hashcode",hashCode);


        //Table API
        Table resultTable = sensorTable
                .select("id,ts,hashcode(id)");
//        resultTable.printSchema();

        //SQL
        tableEnv.createTemporaryView("sensor",sensorTable);
        Table SQLResult = tableEnv.sqlQuery("select id,ts,hashcode(id) from sensor");

        //输出
//        tableEnv.toAppendStream(resultTable, Row.class).print();
        tableEnv.toAppendStream(SQLResult,Row.class).print();

        env.execute();
    }

    //自定义一个求hash code的标量函数
    public static class HashCode extends ScalarFunction{
        public Double factor;

        public HashCode(Double factor) {
            this.factor = factor;
        }

        public int eval(String value){
            return (int) (value.hashCode() * this.factor);
        }
    }
}
