package com.gzl0ng.apitest.tabletest.udftest;

import com.gzl0ng.com.gzl0ng.apitest.beans.SensoReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Upper;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;

/**
 * @author 郭正龙
 * @date 2021-12-06
 */
//自定义聚合函数
public class AggregateFunctionTest {
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

        //流转换成表
        Table sensorTable = tableEnv.fromDataStream(dataStream,"id,temperature,timestamp.rowtime as ts");

        //注册聚合函数
        AvgTemp avgTemp = new AvgTemp();
        tableEnv.registerFunction("avgTemp",avgTemp);

        Table resultTable = sensorTable
                .groupBy("id")
                .aggregate("AvgTemp(temperature) as avgtemp")
                .select("id,avgtemp");

        tableEnv.toRetractStream(resultTable, Row.class).print();
        env.execute();
    }

    //自定义一个聚合函数 泛型<输出类型，累加器>
    public static class AvgTemp extends AggregateFunction<Double, Tuple2<Double,Integer>>{

        @Override
        public Double getValue(Tuple2<Double, Integer> accumulator) {
            return accumulator.f0/accumulator.f1;
        }

        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2(0.0,0);
        }

        public static void accumulate(Tuple2<Double,Integer> acc,Double temp){
            acc.f0 += temp;
            acc.f1 += 1;
        }
    }
}
