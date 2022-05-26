package com.gzl0ng.apitest.window;

import com.gzl0ng.com.gzl0ng.apitest.beans.SensoReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.Tuple2;

/**
 * @author 郭正龙
 * @date 2021-11-03
 */
public class WindowTest2_CountWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //socket文本流
        DataStream<String> inputStream = env.socketTextStream("node1", 7777);

        //转换成SensorReading
        DataStream<SensoReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensoReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        //开计数窗口测试
        DataStream<Double> avgTempResultStream = dataStream.keyBy("id")
                .countWindow(10, 2)
                .aggregate(new MyAvgTemp());

        avgTempResultStream.print();
        env.execute();
    }


    public static class MyAvgTemp implements AggregateFunction<SensoReading, Tuple2<Double,Integer>,Double>{

        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0,0);
        }

        @Override
        public Tuple2<Double, Integer> add(SensoReading value, Tuple2<Double, Integer> accumulator) {
            return new Tuple2<>(accumulator._1 + value.getTemperature(),accumulator._2 + 1);
        }

        @Override
        public Double getResult(Tuple2<Double, Integer> accumulator) {
            return accumulator._1 / accumulator._2;
        }

        @Override
        public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> a, Tuple2<Double, Integer> b) {
            return new Tuple2<>(a._1 + b._1,a._2 + b._2);
        }
    }
}
