package com.gzl0ng.apitest.state;

import com.gzl0ng.com.gzl0ng.apitest.beans.SensoReading;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 郭正龙
 * @date 2021-11-07
 */
public class StateTest4_FaultTolerance {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.状态后端配置
        env.setStateBackend(new MemoryStateBackend());//测试用的多，速度最快
//        env.setStateBackend(new FsStateBackend("hdfs://node1:9000/flink-checkpoints"));
//        env.setStateBackend(new RocksDBStateBackend("hdfs:"));

        //2.检查点配置,默认500毫秒触发保存一次，第一个参数为时间间隔设置，第二个为语义一致性（kafka的至少一次至多一次）
        env.enableCheckpointing(300L);
        //高级选项
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);//超时时间设置
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);//并行保存checkpoints
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(100L);//二次checkpoints之间的间隔时间
        env.getCheckpointConfig().setPreferCheckpointForRecovery(true);//是否跟倾向于使用checkpoints恢复，默认为false系统使用最近的
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(0);//容忍checkpoints失败多少次

        //3.重启策略配置
        //固定延迟重启
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,10000L));//每隔10秒重启一次，超过3此重启失败
        //失败率重启
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.minutes(10),Time.minutes(1)));//10分钟内重启3次，没起来就是挂了


        DataStream<String> inputStream = env.socketTextStream("node1", 7777);

        //转换成SensorReading
        DataStream<SensoReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensoReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        env.execute();
    }
}
