package com.gzl0ng.apitest.tableapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

/**
 * @author 郭正龙
 * @date 2021-11-08
 */
public class TableTest3_FileOutput {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.表的创建：连接外部系统，读取数据
        //2.1读取文件
        String filePath = "E:\\IntelliJ_work\\Flinktutorial\\src\\main\\resources\\sensor.txt";
        tableEnv.connect(new FileSystem().path(filePath))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp",DataTypes.BIGINT())
                        .field("temp",DataTypes.DOUBLE())
                )
                .createTemporaryTable("inputTable");

        Table inputTable = tableEnv.from("inputTable");
//        inputTable.printSchema();
//        tableEnv.toAppendStream(inputTable, Row.class).print();

        //3.查询转换
        //3.1 Table API
        //简单转换
        Table resultTable = inputTable.select("id,temp")
                .filter("id === 'sensor_6'");

        //聚合统计
        Table aggTable = inputTable.groupBy("id")
                .select("id,id.count as count,temp.avg as avgTemp");

        //3.2 SQL
        tableEnv.sqlQuery("select id,temp from inputTable where id ='sensor_6'");
        Table sqlAggTable = tableEnv.sqlQuery("select id,count(id) as cnt,avg(temp) as avgTemp from inputTable group by id");

        //4.输出到文件
        //连接外部环境,注册输出表
        String outputPath = "E:\\IntelliJ_work\\Flinktutorial\\src\\main\\resources\\out.txt";
        tableEnv.connect(new FileSystem().path(outputPath))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("temperature",DataTypes.DOUBLE())
                )
                .createTemporaryTable("outputTable");

        resultTable.insertInto("outputTable");
//        aggTable.insertInto("outputTable");对于进行了聚合操作的不能以追加方式放入文件(状态改变了,跟聚合结果要调用toRetractStream方法打印输出)

        env.execute();
    }
}
