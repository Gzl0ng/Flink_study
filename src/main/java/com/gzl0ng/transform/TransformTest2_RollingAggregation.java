package com.gzl0ng.transform;


import com.gzl0ng.com.gzl0ng.apitest.beans.SensoReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 郭正龙
 * @date 2021-11-01
 */
public class TransformTest2_RollingAggregation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inpuStream = env.readTextFile("E:\\IntelliJ_work\\Flinktutorial\\src\\main\\resources\\sensor.txt");

        //转换成Sensorreading类型
//        DataStream<SensoReading> dataStream = inpuStream.map(new MapFunction<String, SensoReading>() {
//            @Override
//            public SensoReading map(String value) throws Exception {
//                String[] fields = value.split(",");
//                return new SensoReading(fields[0],new Long(fields[1]),new Double(fields[2]));
//            }
//        });

        DataStream<SensoReading> dataStream = inpuStream.map(line -> {
            String[] fields = line.split(",");
            return new SensoReading(fields[0],new Long(fields[1]),new Double(fields[2]));
        });

        //分组
        KeyedStream<SensoReading, Tuple> keyedStream = dataStream.keyBy("id");

//        KeyedStream<SensoReading, String> keyedStream1 = dataStream.keyBy(SensoReading :: getId);
//        KeyedStream<SensoReading, String> keyedStream1 = dataStream.keyBy(data -> data.getId());

        //滚动聚合，取当前最大的温度值,maxby的分区字段会随最大值改变而改变
        DataStream<SensoReading> resultStream = keyedStream.max("temperature");
//        DataStream<SensoReading> resultStream = keyedStream.maxBy("temperature");

        resultStream.print();

        env.execute();
    }
}
