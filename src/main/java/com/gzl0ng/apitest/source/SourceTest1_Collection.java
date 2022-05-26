package com.gzl0ng.apitest.source;

import com.gzl0ng.com.gzl0ng.apitest.beans.SensoReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author 郭正龙
 * @date 2021-11-01
 */
public class SourceTest1_Collection {
    public static void main(String[] args) throws Exception{
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //这样设置全局并行度为1，随机有序输出二条流数据
        env.setParallelism(1);

        //从集合中读取数据
        DataStream<SensoReading> dataStream = env.fromCollection(Arrays.asList(
                new SensoReading("sensor_1", 1547718199L, 35.8),
                new SensoReading("sensor_6", 1547718201L, 15.4),
                new SensoReading("sensor_7", 1547718202L, 6.7),
                new SensoReading("sensor_10", 1547718205L, 38.1)
        ));

        DataStream<Integer> integerDataStream = env.fromElements(1, 2, 3, 4, 5,6,7);

        //打印输出
        dataStream.print("data");
//        integerDataStream.print("int").setParallelism(1);如果这样设置会和上部的读取source合并，一个线程输出
        integerDataStream.print("int");

        //执行
        env.execute();
    }
}
