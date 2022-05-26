package com.gzl0ng.transform;

import com.gzl0ng.com.gzl0ng.apitest.beans.SensoReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 郭正龙
 * @date 2021-11-02
 */
public class TransformTest6_Partition {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStreamSource<String> inputStream = env.readTextFile("E:\\IntelliJ_work\\Flinktutorial\\src\\main\\resources\\sensor.txt");
        DataStream<SensoReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensoReading(fields[0],new Long(fields[1]),new Double(fields[2]));
        });

        inputStream.print("input");

        //1.shuffle
        DataStream<String> shuffleStream = inputStream.shuffle();
//        shuffleStream.print("shuffle");

        //2.keyby
//        dataStream.keyBy("id").print("keyby");

        //3.global
        dataStream.global().print("global");

        env.execute();
    }
}
