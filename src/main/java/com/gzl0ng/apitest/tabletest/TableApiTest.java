package com.gzl0ng.apitest.tabletest;

import com.gzl0ng.com.gzl0ng.apitest.beans.SensoReading;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.types.Row;

import java.net.URL;
import java.util.stream.Stream;

/**
 * @author 郭正龙
 * @date 2021-12-04
 */
public class TableApiTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.创建表执行环境(默认老版本planner)
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        
//        //1.1 老版本planner的流式查询
//        EnvironmentSettings build = EnvironmentSettings.newInstance()
//                .useOldPlanner() //老版本
//                .inStreamingMode() //流处理
//                .build();
//        StreamTableEnvironment oldStreamTableEnv = StreamTableEnvironment.create(env, build);
//        //1.2 老版本批处理环境
//        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
//        BatchTableEnvironment batchTableEnv = BatchTableEnvironment.create(batchEnv);
//
//        //1.3 blink版本流式查询
//        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance()
//                .useBlinkPlanner()
//                .inStreamingMode()
//                .build();
//        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(env, bsSettings);
//        //1.4 blink版本批量查询
//        EnvironmentSettings bbSettings = EnvironmentSettings.newInstance()
//                .useBlinkPlanner()
//                .inBatchMode()
//                .build();
//        TableEnvironment bbTableEnv = TableEnvironment.create(bbSettings);

        //2. 连接外部系统，读取数据
        //2.1 读取文件数据
        String filePath = TableApiTest.class.getResource("/sensor.txt").getPath();
        tableEnv.connect(new FileSystem().path(filePath))
                .withFormat(new OldCsv())    //定义了从外部文件读取数据之后的格式化方法
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp",DataTypes.BIGINT())
                        .field("temperature",DataTypes.DOUBLE())
                )   //定义表结构
                .createTemporaryTable("inputTable"); //在表环境Catalog里注册一张表

        //2.2消费kafka数据
        tableEnv.connect(new Kafka()
        .version("0.11")  //定义kafka版本
        .topic("sensor")  //定义主题
        .property("zookeeper.connect","node1:2181")
        .property("bootstrap.servers","node1:9092")
        )
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp",DataTypes.BIGINT())
                        .field("temperature",DataTypes.DOUBLE())
                )
                .createTemporaryTable("kafkaInputTable");

        //3. 表的查询转换
        Table sensorTable = tableEnv.from("inputTable");
        //3.1 简单查询转换
        Table resultTable = sensorTable
                .select("id,temperature")
                .filter("id === 'sensor_1'");
//        tableEnv.toAppendStream(resultTable,Row.class).print();
        //3.2 聚合转换
        Table aggResultTable = sensorTable
                .groupBy("id")
                .select("id,id.count as cnt");
//        tableEnv.toRetractStream(aggResultTable,Row.class).print();

        Table aggTable = tableEnv.sqlQuery("select id,count(id) as cnt from inputTable group by id");
        tableEnv.toRetractStream(aggTable,Row.class).print();

        //测试输出
//        Table inputTable = tableEnv.from("inputTable");
        Table inputTable = tableEnv.from("kafkaInputTable");
//        tableEnv.toAppendStream(inputTable,Row.class).print();

        env.execute();
    }
}
