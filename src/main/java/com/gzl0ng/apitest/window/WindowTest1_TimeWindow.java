package com.gzl0ng.apitest.window;

import akka.stream.javadsl.Merge;
import com.gzl0ng.com.gzl0ng.apitest.beans.SensoReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author 郭正龙
 * @date 2021-11-03
 */
public class WindowTest1_TimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从文件读取数据
        DataStream<String> inputStream = env.readTextFile("E:\\IntelliJ_work\\Flinktutorial\\src\\main\\resources\\sensor.txt");

        //socket文本流
//        DataStream<String> inputStream = env.socketTextStream("node1", Integer.parseInt("7777"));

        //转换成SensorReading
        DataStream<SensoReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensoReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        //开窗测试

        //1.增量函数
        DataStream<Integer> resultStream = dataStream.keyBy("id")
//                .countWindow(10,2);
//                .window(EventTimeSessionWindows.withGap(Time.minutes(15)));
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
                .timeWindow(Time.seconds(15))
                .aggregate(new AggregateFunction<SensoReading, Integer, Integer>() {
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(SensoReading value, Integer accumulator) {
                        return accumulator + 1;
                    }

                    @Override
                    public Integer getResult(Integer accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return a + b;
                    }
                });

        //2.全窗口函数
        DataStream<Tuple3<String,Long,Integer>> resultStream2 = dataStream.keyBy("id")
                .timeWindow(Time.seconds(15))
                .apply(new WindowFunction<SensoReading, Tuple3<String,Long,Integer>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<SensoReading> input, Collector<Tuple3<String,Long,Integer>> out) throws Exception {
                        String id = tuple.getField(0);
                        Long windowEnd = window.getEnd();
                        Integer count = IteratorUtils.toList(input.iterator()).size();
                        out.collect(new Tuple3<>(id,windowEnd,count));
                    }
                });

        //3.其他可选API
        OutputTag outputTag = new OutputTag<SensoReading>("late") {
        };

        SingleOutputStreamOperator<SensoReading> sumStream = dataStream.keyBy("id")
                .timeWindow(Time.seconds(15))
//                .trigger()
//                .evictor()
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(outputTag)
                .sum("temperature");

        resultStream.print("r1");
        resultStream2.print("r2");
        sumStream.getSideOutput(outputTag).print("sum");
        env.execute();
    }
}
