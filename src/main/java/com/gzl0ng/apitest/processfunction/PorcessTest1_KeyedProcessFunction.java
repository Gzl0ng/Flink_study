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

/**
 * @author 郭正龙
 * @date 2021-11-07
 */
public class PorcessTest1_KeyedProcessFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.socketTextStream("node1", 7777);

        //转换成SensorReading
        DataStream<SensoReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensoReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        //测试KeyedProcessFunction，先分组然后自定义处理
        dataStream.keyBy("id")
                .process(new MyProcessFunction())
                .print();

        env.execute();
    }

    //实现自定义的处理函数,定时处理针对key有效
    public static class MyProcessFunction extends KeyedProcessFunction<Tuple,SensoReading,Integer>{
        ValueState<Long> tsTimerState;
        @Override
        public void open(Configuration parameters) throws Exception {
            tsTimerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-timer",Long.class));
        }

        @Override
        public void processElement(SensoReading value, Context ctx, Collector<Integer> out) throws Exception {
            out.collect(value.getId().length());

            //context
            ctx.timestamp();//本地时间戳获取
            ctx.getCurrentKey();
//            ctx.output();
            ctx.timerService().currentProcessingTime();
            ctx.timerService().currentWatermark();
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 10000L);//这里指这条数据处理后的10秒定时任务开启
            tsTimerState.update(ctx.timerService().currentProcessingTime() + 10000L);//将这个定时状态存储
//            ctx.timerService().registerEventTimeTimer((value.getTimestamp() + 10) * 1000L);//定时开始时间
//            ctx.timerService().deleteEventTimeTimer(tsTimerState.value());//这里指 定时服务结束时间
        }

        //定时服务触发操作
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Integer> out) throws Exception {
            System.out.println(timestamp + "定时器触发");
            ctx.getCurrentKey();
//            ctx.output();
            ctx.timeDomain();//得到定时服务的类型是处理事件时间还是process时间
        }

        @Override
        public void close() throws Exception {
            tsTimerState.clear();
        }
    }
}
