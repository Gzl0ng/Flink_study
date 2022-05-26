package com.gzl0ng.apitest.processfunction;

import com.gzl0ng.com.gzl0ng.apitest.beans.SensoReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author 郭正龙
 * @date 2021-11-07
 */
public class ProcessTest3_SideOutputCase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.socketTextStream("node1", 7777);

        //转换成SensorReading
        DataStream<SensoReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensoReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        //定义一个OutputTag，用来表示侧输出流（低温流）
        OutputTag<SensoReading> lowTempTag = new OutputTag<SensoReading>("lowtemp") {
        };
        OutputTag<Double> intlowtemp = new OutputTag<Double>("intlowtemp"){};

        //测试ProcessFunction，自定义侧输出流实现分流操作
        SingleOutputStreamOperator<SensoReading> highTempStream = dataStream.process(new ProcessFunction<SensoReading, SensoReading>() {
            @Override
            public void processElement(SensoReading value, Context ctx, Collector<SensoReading> out) throws Exception {
                //判断温度大于30度，高温流输出到主流，小于30度输出到侧输出流
                if (value.getTemperature() > 30){
                    out.collect(value);
                }else {
                    ctx.output(lowTempTag,value);
                    ctx.output(intlowtemp,value.getTemperature());
                }
            }
        });
           highTempStream.print("high-temp");
           highTempStream.getSideOutput(lowTempTag).print("low-temp");
           highTempStream.getSideOutput(intlowtemp).print("int-low-temp");

        env.execute();
    }
}
