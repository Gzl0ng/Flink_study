package com.gzl0ng.apitest.source;

import com.gzl0ng.com.gzl0ng.apitest.beans.SensoReading;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.sources.wmstrategies.WatermarkStrategy;

import java.util.Properties;

/**
 * @author 郭正龙
 * @date 2021-11-01
 */
public class SourceTest3_Kafka {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","node1:9092");
        properties.setProperty("group.id", "consumer-group");
//        properties.setProperty("key.deserializer",
//                "org.apache.kafka.common.serialization.StringDeserializer");
//        properties.setProperty("value.deserializer",
//                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        //开启动态分区，如果kafka加了分区数，这里会每隔一段时间检测
        properties.setProperty("FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS",30 * 1000 + "");
        //读取数据
        FlinkKafkaConsumer011<String> kafkaConsumer011 = new FlinkKafkaConsumer011<>("sensor", new SimpleStringSchema(), properties);
//        kafkaConsumer011.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundeOutOfOrderness(duration.ofSeconds(3)))
//        kafkaConsumer011
//                .setStartFromGroupOffsets()   这里是默认从offset消费，没有就从property指定的消费
//        .setStartFromEarliest() 这样指定是强制从最早的消费
//        .setStartFromLatest()  这样指定是强制最后的消费
//        .setStartFromTimestamp()

        DataStreamSource<String> dataStream = env.addSource(kafkaConsumer011);

        SingleOutputStreamOperator<SensoReading> resultStream = dataStream.map(line -> {
            String[] fields = line.split(",");
            return new SensoReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<SensoReading>() {
            @Override
            public long extractAscendingTimestamp(SensoReading element) {
                return element.getTimestamp() * 1000L;
            }
        });

        resultStream.print();

        env.execute();
    }
}
