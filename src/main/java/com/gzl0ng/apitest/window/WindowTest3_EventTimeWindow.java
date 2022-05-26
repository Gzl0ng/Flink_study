package com.gzl0ng.apitest.window;

import com.gzl0ng.com.gzl0ng.apitest.beans.SensoReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

/**
 * @author 郭正龙
 * @date 2021-11-03
 */
public class WindowTest3_EventTimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置自动生成watermark的周期
        env.getConfig().setAutoWatermarkInterval(100);

        DataStream<String> inputStream = env.socketTextStream("node1", 7777);

        //转换成SensorReading
        DataStream<SensoReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensoReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        })
                .setParallelism(4)
                //正常排序，设置事件时间
//                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<SensoReading>() {
//                    @Override
//                    public long extractAscendingTimestamp(SensoReading element) {
//                        return element.getTimestamp() * 1000;
//                    }
//                })
                //乱序数据，设置时间戳和Watermark
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensoReading>(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(SensoReading element) {
                        return element.getTimestamp() * 1000;
                    }
                });


        OutputTag<SensoReading> outputTag = new OutputTag<SensoReading>("late"){
        };

        //基于事件时间的开窗聚合，统计15秒内温度的最小值
        SingleOutputStreamOperator<SensoReading> minTempStream = dataStream.keyBy("id")
                .timeWindow(Time.seconds(15))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(outputTag)
                .minBy("temperature");

        minTempStream.print("minTemp");
        minTempStream.getSideOutput(outputTag).print();

        env.execute();
    }
}
