package com.gzl0ng.apitest.state;

import com.gzl0ng.com.gzl0ng.apitest.beans.SensoReading;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import scala.Tuple3;


/**
 * @author 郭正龙
 * @date 2021-11-06
 */
public class StateTest3_KeyedStateApplicationCase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.socketTextStream("node1", 7777);

        //转换成SensorReading
        DataStream<SensoReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensoReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        //定义一个flatmap操作，检测温度跳变，输出报警
        SingleOutputStreamOperator<Tuple3<String,Double,Double>> resultStream = dataStream.keyBy("id")
                .flatMap(new TempChangeWarning(10.0));

        resultStream.print();
        env.execute();
    }

    //实现自定义函数类
    public static class TempChangeWarning extends RichFlatMapFunction<SensoReading, Tuple3<String,Double,Double>> {
        //私有属性，温度跳变阈值
        private Double threshold;

        //定义状态，保存上一次的温度值
        private ValueState<Double> lastTempState;

        public TempChangeWarning(Double threshold) {
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp", Double.class));
        }


        @Override
        public void flatMap(SensoReading value, Collector<Tuple3<String, Double, Double>> out) throws Exception {
            //获取状态
            Double lastTemp = lastTempState.value();
            //如果状态不为null，那么就判断二次温度差值
            if (lastTemp != null){
                Double diff = Math.abs(value.getTemperature() - lastTemp);
                if (diff >= threshold){
                    out.collect(new Tuple3<>(value.getId(),lastTemp,value.getTemperature()));
                }
            }

            //更新状态
            lastTempState.update(value.getTemperature());
        }

        @Override
        public void close() throws Exception {
            lastTempState.clear();
        }
    }
}
