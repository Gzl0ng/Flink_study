package com.gzl0ng.apitest.processfunction;

import com.gzl0ng.com.gzl0ng.apitest.beans.SensoReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.zookeeper.Op;

/**
 * @author 郭正龙
 * @date 2021-11-07
 */
public class ProcessTest2_ApplicationCase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.socketTextStream("node1", 7777);

        //转换成SensorReading
        DataStream<SensoReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensoReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        dataStream.keyBy("id")
                .process(new TempConsIncreWarning(10))
                .print();

        env.execute();
    }

    //实现自定义处理函数，检测一段时间内的温度连续上升，输出报警‘
    public static class TempConsIncreWarning extends KeyedProcessFunction<Tuple,SensoReading,String>{
        //定义私有属性,当前统计的时间间隔
        private Integer interval;

        //定义状态，保存上一个的温度值，定时器时间戳
        private ValueState<Double> lastTempState;
        private ValueState<Long> timerTsState;

        public TempConsIncreWarning(Integer interval) {
            this.interval = interval;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp",Double.class,Double.MIN_VALUE));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-ts",Long.class));
        }

        @Override
        public void processElement(SensoReading value, Context ctx, Collector<String> out) throws Exception {
            //取出状态
            Double lastTemp = lastTempState.value();
            Long timerTs = timerTsState.value();

            //如果温度上升并且没有定时器的时候，注册10秒后的定时器，开始等待
            if (value.getTemperature() > lastTemp && timerTs == null){
                //计算出定时器时间戳
                Long ts = ctx.timerService().currentProcessingTime() + interval * 1000;
                ctx.timerService().registerProcessingTimeTimer(ts);
                timerTsState.update(ts);
            }
            //如果温度下降，那么删除定时器
            else if (value.getTemperature() < lastTemp && timerTs != null){
                ctx.timerService().deleteProcessingTimeTimer(timerTs);
                timerTsState.clear();
            }

            //更新温度状态
            lastTempState.update(value.getTemperature());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //定时器触发,删除报警信息
            out.collect("传感器" + ctx.getCurrentKey().getField(0) + "温度值连续" + interval +  "s上升");
        }

        @Override
        public void close() throws Exception {
            lastTempState.clear();
        }
    }
}
