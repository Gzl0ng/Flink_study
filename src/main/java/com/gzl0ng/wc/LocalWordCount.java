package com.gzl0ng.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * @author 郭正龙
 * @date 2021-12-07
 */
public class LocalWordCount {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

//        RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend("hdfs://node1:9000/flinktest/checkpoint", true);
        MemoryStateBackend memoryStateBackend = new MemoryStateBackend();
        env.setStateBackend(memoryStateBackend);

        //开启Checkpoint
        env.enableCheckpointing(1000);

        //高级选项
        //配置Checkpoint
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //确认 checkpoints之间的时间会进行500ms
        checkpointConfig.setMinPauseBetweenCheckpoints(500);
        //超时时间1分钟,超时此次check丢掉
        checkpointConfig.setCheckpointTimeout(60000);
        //同一时间只允许一个checkpoint进行
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        //开启在job终止后仍然保留的externalized checkpoints
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);


        //从文件读取数据,datasource是一个算子最后是继承dataset数据集
//        String inputPath = "E:\\atguigu\\source\\Flinktutorial\\src\\main\\resources\\hello.txt";
//        DataSet<String> inputDataSet = env.readTextFile(inputPath);
        DataStream<String> inputStream = env.socketTextStream("node1", 7777);

        //对数据集进行处理,按空格分词展开，转换成（word，1）二元组进行统计
        DataStream<Tuple2<String,Integer>> resultSet =  inputStream.flatMap(new WordCount.MyFlatMapper())
                .keyBy(0)//按照第一个位置的word分组
                .sum(1);//将第二个位置上的数据求和

        resultSet.print();

//        ParameterTool parameterTool = ParameterTool.fromArgs(args);
//        String jobname = parameterTool.get("jobname");

        ParameterTool parameterTool = ParameterTool.fromSystemProperties();
        System.out.println(parameterTool.toMap().toString());

        env.execute();
    }

    //自定义类，实现FlatMapFunction接口
    public  static class  MyFlatMapper implements FlatMapFunction<String, Tuple2<String,Integer>> {


        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            //按空格分组
            String[] words = value.split(" ");
            //遍历所有word，包成二元组输出
            for (String word : words) {
                out.collect(new Tuple2<>(word,1));
            }
        }
    }
}
