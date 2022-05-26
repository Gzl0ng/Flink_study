package com.gzl0ng.apitest.tabletest;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

/**
 * @author 郭正龙
 * @date 2021-12-05
 */
public class KafkaPipLineTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.创建表执行环境
        StreamTableEnvironment tablenv = StreamTableEnvironment.create(env);

        //2.定义到kafka的连接创建
        tablenv.connect(new Kafka()
        .version("0.11")
        .topic("sensor")
        .property("zookeeper.connect","node1:2181")
        .property("bootstrap.servers","node1:9092"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                .field("id", DataTypes.STRING())
                .field("timestamp",DataTypes.BIGINT())
                .field("temperature",DataTypes.DOUBLE()))
                .createTemporaryTable("inputTable");

        Table inputable = tablenv.from("inputTable");
        Table resultTable = inputable
                .select("id,temperature")
                .filter("id = 'sensor_1'");
        Table aggResultTable = inputable
                .groupBy("id")
                .select("id,id.count as cnt");

        tablenv.connect(new Kafka()
        .version("0.11")
        .topic("sinktest")
        .property("zookeeper.connect","node1:2181")
        .property("bootstrap.servers","node1:9092"))
                .withFormat(new Csv().lineDelimiter(""))
                .withSchema(new Schema()
                .field("id",DataTypes.STRING())
                .field("temp",DataTypes.DOUBLE()))
                .createTemporaryTable("kafkaOutputTable");

        tablenv.connect(new Kafka()
                .version("0.11")
                .topic("sinktest")
                .property("zookeeper.connect","node1:2181")
                .property("bootstrap.servers","node1:9092"))
                .withFormat(new Csv().lineDelimiter(""))
                .withSchema(new Schema()
                        .field("id",DataTypes.STRING())
                        .field("cnt",DataTypes.BIGINT()))
                .createTemporaryTable("kafkaOutputTable2");

        resultTable.insertInto("kafkaOutputTable");
//        aggResultTable.insertInto("kafkaOutputTable2"); kafka仅支持只追加模式，聚合结果输出到kafka会报错(es,hbase等数据库支持修改)



        env.execute("kafka pipeline test job");
    }
}
