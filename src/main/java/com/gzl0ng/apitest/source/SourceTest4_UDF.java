package com.gzl0ng.apitest.source;

import com.gzl0ng.com.gzl0ng.apitest.beans.SensoReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

/**
 * @author 郭正龙
 * @date 2021-11-01
 */

public class SourceTest4_UDF {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<SensoReading> dataStream = env.addSource(new MySensorSource());

        dataStream.print();

        env.execute();
    }

    //实现自定义的sourceFunction
    public static class MySensorSource implements SourceFunction<SensoReading> {
        //定义一个标志位，用来控制数据的产生
        private boolean running = true;


        @Override
        public void run(SourceContext sourceContext) throws Exception {
            //定义一个随机数发生器
            Random random = new Random();

            //设置10个传感器的初始温度值
            HashMap<String, Double> sensortempMap = new HashMap<>();
            for (int i = 0; i < 10; i++) {
                //高斯分布，就是正太分布,这里是+-3之间
                sensortempMap.put("sensor_" + (i + 1) , 60 + random.nextGaussian() * 20);
            }

            while (running) {
                for (String sensorId : sensortempMap.keySet()){
                    //在当前温度基础上做一个随机波动
                    Double newtemp = sensortempMap.get(sensorId) + random.nextGaussian();
                    sensortempMap.put(sensorId,newtemp);
                    sourceContext.collect(new SensoReading(sensorId,System.currentTimeMillis(),newtemp));
                }
                //控制输出频率

                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
