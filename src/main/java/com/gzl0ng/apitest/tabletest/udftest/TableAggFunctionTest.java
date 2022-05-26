package com.gzl0ng.apitest.tabletest.udftest;

import com.gzl0ng.com.gzl0ng.apitest.beans.SensoReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * @author 郭正龙
 * @date 2021-12-06
 */

//表聚合函数
public class TableAggFunctionTest {
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

        //创建一个表聚合函数的实例
        Top2Temp top2Temp = new Top2Temp();
        tableEnv.registerFunction("top2Temp", top2Temp);

        Table resultTable = sensorTable
                .groupBy("id")
                .flatAggregate("temperature.top2Temp as (temp,rank)")
                .select("id,temp,rank");

//        tableEnv.toAppendStream(sensorTable,Row.class).print();
        tableEnv.toRetractStream(resultTable, Row.class).print();

        env.execute();
    }

    //自定义一个表聚合函数,实现top2功能,输出（temp，rank）

    /**
     * 这里使用一个tuple2对象存储第一高温和第二高温
     * 每来一条数据就和tuple2的属性进行比较，大于其中的就替换
     * 每来一条数据在比较后会输出tuple2里的属性(封装成另一个tuple2<温度，温度排名>)
     */
    public static class Top2Temp extends TableAggregateFunction<Tuple2<Double, Integer>, Tuple2<Double, Double>> {

        //初始化状态
        @Override
        public Tuple2<Double, Double> createAccumulator() {
            return new Tuple2<>(0.0, 0.0);
        }

        //每来一个数据后，聚合计算的操作
        public void accumulate(Tuple2<Double, Double> acc, Double temp) {
            //将当前温度值，根状态中的最高温和第二温比较，如果大于就替换
            if (temp > acc.f0) {
                //如果比最高温高，就排第一，其它温度依次后移
                acc.f1 = acc.f0;
                acc.f0 = temp;
            } else if (temp > acc.f1) {
                acc.f1 = temp;
            }
        }

        //实现一个输出数据的方法,写入结果表中
        public void emitValue(Tuple2<Double, Double> acc, Collector<Tuple2<Double, Integer>> out) {
            out.collect(new Tuple2<>(acc.f0, 1));
            out.collect(new Tuple2<>(acc.f1, 2));
        }
    }
}
