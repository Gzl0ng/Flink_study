package com.gzl0ng.transform;

import com.gzl0ng.com.gzl0ng.apitest.beans.SensoReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 郭正龙
 * @date 2021-11-01
 */
public class TransformTest3_Reduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.readTextFile("E:\\IntelliJ_work\\Flinktutorial\\src\\main\\resources\\sensor.txt");

        DataStream<SensoReading> dataStream = inputStream.map(new MapFunction<String, SensoReading>() {
            @Override
            public SensoReading map(String value) throws Exception {
                String[] fields = value.split(",");
                return new SensoReading(fields[0], new Long(fields[1]), new Double(fields[2]));
            }
        });

        KeyedStream<SensoReading, Tuple> keyedStream = dataStream.keyBy("id");

//        DataStream<SensoReading> resultStream = keyedStream.reduce(new ReduceFunction<SensoReading>() {
//            @Override
//            public SensoReading reduce(SensoReading value1, SensoReading value2) throws Exception {
//                return new SensoReading(value1.getId(),value2.getTimestamp(),Math.max(value1.getTemperature(),value2.getTemperature()));
//            }
//        });

        DataStream<SensoReading> resultStream = keyedStream.reduce((value1, value2) -> new SensoReading(value1.getId(), value2.getTimestamp(), Math.max(value1.getTemperature(), value2.getTemperature())));

        resultStream.print();
        env.execute();
    }
}
