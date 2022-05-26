package com.gzl0ng.transform;

import com.gzl0ng.com.gzl0ng.apitest.beans.SensoReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.Tuple2;

/**
 * @author 郭正龙
 * @date 2021-11-02
 */
public class TransformTest5_RichFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStream<String> inputStream = env.readTextFile("E:\\IntelliJ_work\\Flinktutorial\\src\\main\\resources\\sensor.txt");

        //转换
        SingleOutputStreamOperator<SensoReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensoReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        DataStream<Tuple2<String,Integer>> resultStream = dataStream.map(new MyMapper());

        resultStream.print();
        env.execute();
    }

//    public static class  MyMapper implements MapFunction<SensoReading,Tuple2<String,Integer>>{
//
//        @Override
//        public Tuple2<String, Integer> map(SensoReading value) throws Exception {
//            return new Tuple2<>(value.getId(),value.getId().length());
//        }
//    }

    //实现自定义的富函数类
    public static class  MyMapper extends RichMapFunction<SensoReading,Tuple2<String,Integer>>{
        @Override
        public Tuple2<String, Integer> map(SensoReading value) throws Exception {
//            ValueState<String> state = getRuntimeContext().getState(new ValueStateDescriptor<String>("state", String.class));

            return new Tuple2<>(value.getId(),getRuntimeContext().getIndexOfThisSubtask());
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            //初始化工作，一般是定义状态或者建立数据库连接
            System.out.println("open");
        }

        @Override
        public void close() throws Exception {
            //关闭连接和清空状态的收尾操作
            System.out.println("close");

        }
    }
}
