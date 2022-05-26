package com.gzl0ng.apitest.tabletest;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import java.net.URL;

/**
 * @author 郭正龙
 * @date 2021-12-04
 */
public class FsOutPutTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.直接成view表
        String path = "E:\\IntelliJ_work\\Flinktutorial\\src\\main\\resources\\sensor.txt";
        tableEnv.connect(new FileSystem().path(path))
                .withFormat(new Csv())
                .withSchema(new Schema()
                    .field("id", DataTypes.STRING())
                    .field("ts",DataTypes.BIGINT())
                    .field("temperature",DataTypes.DOUBLE()))
                .createTemporaryTable("input");

//        Table input = tableEnv.from("input");

        Table resultTable = tableEnv.sqlQuery("select id,temperature from input where id = 'sensor_1'");
        Table aggResultTable = tableEnv.sqlQuery("select id,count(id) as cnt from input group by id");

//        tableEnv.toAppendStream(resultTable,Row.class).print();
//        tableEnv.toRetractStream(aggResultTable,Row.class).print();

        String path1 = "E:\\IntelliJ_work\\Flinktutorial\\src\\main\\resources\\output.txt";
        //将结果表输出到文件
        tableEnv.connect(new FileSystem().path(path1))
                .withFormat(new Csv())
                .withSchema(new Schema()
                    .field("id",DataTypes.STRING())
                    .field("temp",DataTypes.DOUBLE()))
//                    .field("cnt",DataTypes.BIGINT()))
                .createTemporaryTable("outputTable");

        resultTable.insertInto("outputTable");
//        aggResultTable.insertInto("outputTable");  文件不能修改，这个表是一个发生改变的表
        env.execute("fs output test job");
    }
}
