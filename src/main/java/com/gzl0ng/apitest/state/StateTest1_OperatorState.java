package com.gzl0ng.apitest.state;

import com.gzl0ng.com.gzl0ng.apitest.beans.SensoReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;
import java.util.List;

/**
 * @author 郭正龙
 * @date 2021-11-05
 */
public class StateTest1_OperatorState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.socketTextStream("node1", 7777);

        //转换成SensorReading
        DataStream<SensoReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensoReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        //定义一个有状态的map操作，统计当前分区数据个数
        SingleOutputStreamOperator<Integer> resultStream = dataStream.map(new MyCountMapper());
        resultStream.print();

        env.execute();
    }

    //自定义MapFunction
    public static class MyCountMapper implements MapFunction<SensoReading,Integer>, ListCheckpointed<Integer> {
        //定义一个本地变量，作为算子状态
        private Integer count = 0;

        //每来一条数据状态怎么变化
        @Override
        public Integer map(SensoReading value) throws Exception {
            count++;
            return count;
        }

        //对状态快照，记录每一次状态
        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
            return Collections.singletonList(count);
        }

        //故障后调用
        @Override
        public void restoreState(List<Integer> state) throws Exception {
            for (Integer num : state) {
                count += num;
            }
        }
    }
}
