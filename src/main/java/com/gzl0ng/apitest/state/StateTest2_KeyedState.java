package com.gzl0ng.apitest.state;

import com.gzl0ng.com.gzl0ng.apitest.beans.SensoReading;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.PrimitiveIterator;

/**
 * @author 郭正龙
 * @date 2021-11-05
 */
public class StateTest2_KeyedState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.socketTextStream("node1", 7777);

        //转换成SensorReading
        DataStream<SensoReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensoReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        //定义一个有状态的map操作，统计当前sensor数据个数
        DataStream<Integer> resultStream = dataStream
                .keyBy("id")
                .map(new MyKeyCountMapper());

        resultStream.print();
        env.execute();
    }

    //自定义RichMapFunction
    public static class MyKeyCountMapper extends RichMapFunction<SensoReading,Integer>{
        private ValueState<Integer> keyCountState;

        //其他类型状态声明:value state,List state,Map state,Reduce state & Aggregating state
        private ListState<String> myListState;
        private MapState<String,Double> myMapState;
        private ReducingState<SensoReading> myReduingState;

        @Override
        public void open(Configuration parameters) throws Exception {
            //官方不推荐这样给初值，建议判断是否为null再赋值
            keyCountState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("key-count",Integer.class,0));

            //其它类型状态声明
            myListState = getRuntimeContext().getListState(new ListStateDescriptor<String>("my-list",String.class));
            myMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Double>("my-map",String.class,Double.class));
//            myReduingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<SensoReading>());
        }

        @Override
        public Integer map(SensoReading value) throws Exception {
            //其它状态API调用
            //list state
            for (String s : myListState.get()) {
                System.out.println(s);
            }
            myListState.add("hello");
            //mapState
            myMapState.get("1");
            myMapState.put("2",12.3);
            //reduceState
            myReduingState.add(value);

            //公共的方法
            myReduingState.clear();

            //官方建议的设初值方法
//            if (keyCountState.value() == null){
//                keyCountState.update(0);
//            }
            Integer count = keyCountState.value();
            count++;
            keyCountState.update(count);
            return count;
        }
    }

}
