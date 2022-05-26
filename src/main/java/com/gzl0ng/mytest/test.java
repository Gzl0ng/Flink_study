package com.gzl0ng.mytest;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 郭正龙
 * @date 2022-04-27
 */
public class test {
    public static void main(String[] args) {
        System.out.println(test.class.getResource("/").getPath());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        System.out.println(env.getStreamTimeCharacteristic());
    }
}
