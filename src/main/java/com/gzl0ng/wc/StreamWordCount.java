package com.gzl0ng.wc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author 郭正龙
 * @date 2021-10-29
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //并行度设置(默认并行度为电脑核心数，本机8线程,也就是分区编号)
//        env.setParallelism(4);

        //从文件中读取数据
//        String inputPath = "E:\\atguigu\\source\\Flinktutorial\\src\\main\\resources\\hello.txt";
//        DataStreamSource<String> inputDataStream = env.readTextFile(inputPath);

        //用parametertool tool工具从程序启动参数中提取配置项
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");

        //真正的流数据选择kafka
        //从soket文本流读取数据,需要windows环境有linux并且有nc工具
        DataStream<String> inputDataStream = env.socketTextStream(host,port);

        //基于数据流进行转换计算
        DataStream<Tuple2<String, Integer>> resultStream = inputDataStream.flatMap(new WordCount.MyFlatMapper()).slotSharingGroup("green")
                .keyBy(0)
                .sum(1).setParallelism(2).slotSharingGroup("red");
        resultStream.print();

        //执行任务
        env.execute();
    }
}
