package com.gzl0ng.apitest.tableapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

/**
 * @author 郭正龙
 * @date 2021-11-08
 */
public class TableTest4_KafkaPipeLine {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.连接kafka，读取数据
        tableEnv.connect(new Kafka()
        .version("0.11")
        .topic("sensor")
                .property("zookeeper.connect","node1:2181")
                .property("bootstrap.servers","node1:9092")
        )
                .withFormat(new Csv())
                .withSchema(new Schema()
                .field("id",DataTypes.STRING())
                .field("timestamp",DataTypes.BIGINT())
                .field("temp",DataTypes.DOUBLE())
                )
                .createTemporaryTable("inputTable");

        //3.查询转换
        //简单转换
        Table sensorTable = tableEnv.from("inputTable");
        Table resultTable = sensorTable.select("id,temp")
                .filter("id === 'sensor_6'");

        //聚合统计
        Table aggTable = sensorTable.groupBy("id")
                .select("id,id.count as count,temp.avg as avgTemp");

        //4.建立kafka连接，输出到不同的topic下
        tableEnv.connect(new Kafka()
                .version("0.11")
                .topic("sinktest")
                .property("zookeeper.connect","node1:2181")
                .property("bootstrap.servers","node1:9092")
        )
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id",DataTypes.STRING())
//                        .field("timestamp",DataTypes.BIGINT())
                        .field("temp",DataTypes.DOUBLE())
                )
                .createTemporaryTable("outputTable");

        resultTable.insertInto("outputTable");

        env.execute();
    }
}
