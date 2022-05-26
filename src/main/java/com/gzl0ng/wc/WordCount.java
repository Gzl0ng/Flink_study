package com.gzl0ng.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author 郭正龙
 * @date 2021-10-29
 */

//批处理wordcount
public class WordCount {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        
        //从文件读取数据,datasource是一个算子最后是继承dataset数据集
        String inputPath = "E:\\atguigu\\source\\Flinktutorial\\src\\main\\resources\\hello.txt";
        DataSet<String> inputDataSet = env.readTextFile(inputPath);

        //对数据集进行处理,按空格分词展开，转换成（word，1）二元组进行统计
        DataSet<Tuple2<String,Integer>> resultSet =  inputDataSet.flatMap(new MyFlatMapper())
                .groupBy(0)//按照第一个位置的word分组
                .sum(1);//将第二个位置上的数据求和

        resultSet.print();
    }

    //自定义类，实现FlatMapFunction接口
    public  static class  MyFlatMapper implements FlatMapFunction<String, Tuple2<String,Integer>>{


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
