package com.gzl0ng.apitest.sink;

import com.gzl0ng.com.gzl0ng.apitest.beans.SensoReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @author 郭正龙
 * @date 2021-11-02
 */
public class SinkTest2_Redis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.readTextFile("E:\\IntelliJ_work\\Flinktutorial\\src\\main\\resources\\sensor.txt");

        DataStream<SensoReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensoReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

//        定义jedis连接配置
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost("192.168.164.128")
                .setPort(6379)
                .build();

        dataStream.addSink(new RedisSink<>(config,new MyRedisMapper()));

        env.execute();
    }

    //自定义RedisMapper
    public static class MyRedisMapper implements RedisMapper<SensoReading> {
        //定义保存数据到redis的命令，存成hash表,hset sensor_temp id temperature
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET,"sensor_tempe");
        }

        @Override
        public String getKeyFromData(SensoReading sensoReading) {
            return sensoReading.getId();
        }

        @Override
        public String getValueFromData(SensoReading sensoReading) {
            return sensoReading.getTemperature().toString();
        }
    }
}
