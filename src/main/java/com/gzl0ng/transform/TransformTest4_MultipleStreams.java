package com.gzl0ng.transform;

import com.gzl0ng.com.gzl0ng.apitest.beans.SensoReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.util.Collections;

/**
 * @author 郭正龙
 * @date 2021-11-01
 */
public class TransformTest4_MultipleStreams {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //读取
        DataStream<String> inputStream = env.readTextFile("E:\\IntelliJ_work\\Flinktutorial\\src\\main\\resources\\sensor.txt");

        //转换
        DataStream<SensoReading> dataStream = inputStream.map(new MapFunction<String, SensoReading>() {
            @Override
            public SensoReading map(String value) throws Exception {
                String[] fields = value.split(",");
                return new SensoReading(fields[0], new Long(fields[1]), new Double(fields[2]));
            }
        });
        
        //1.分流，按照温度值30度为界分为二条流
        SplitStream<SensoReading> splitStream = dataStream.split(new OutputSelector<SensoReading>() {
            @Override
            public Iterable<String> select(SensoReading value) {
                return (value.getTemperature() > 30) ? Collections.singletonList("high"):Collections.singletonList("low");
            }
        });

        DataStream<SensoReading> highTempStream = splitStream.select("high");
        DataStream<SensoReading> lowTempStream = splitStream.select("low");
        DataStream<SensoReading> allTempSteam = splitStream.select("low","high");

        highTempStream.print("high");
        lowTempStream.print("low");
        allTempSteam.print("all");

        //2.合流 connect,将高温流转换成二元组类型，与低温流连接合并之后输出状态信息
        DataStream<Tuple2<String, Double>> warningStream = highTempStream.map(new MapFunction<SensoReading, Tuple2<String, Double>>() {
            @Override
            public Tuple2 map(SensoReading value) throws Exception {
                return new Tuple2(value.getId(), value.getTemperature());
            }
        });

        ConnectedStreams<Tuple2<String, Double>, SensoReading> connectedStreams = warningStream.connect(lowTempStream);

        DataStream<Object> resultStream = connectedStreams.map(new CoMapFunction<Tuple2<String, Double>, SensoReading, Object>() {
            @Override
            public Object map1(Tuple2<String, Double> value) throws Exception {
                return new Tuple3<>(value._1, value._2, "high temp warning");
            }

            @Override
            public Object map2(SensoReading value) throws Exception {
                return new Tuple2<>(value.getId(), "normal");
            }
        });

        resultStream.print("result");

        //3.union联合多条流,只能union同类型的流
//        warningStream.union(lowTempStream);
        DataStream<SensoReading> union = highTempStream.union(lowTempStream, allTempSteam);
        union.print("union");

        env.execute();
    }
}
