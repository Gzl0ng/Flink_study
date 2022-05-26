package com.gzl0ng.apitest.sink;

import com.gzl0ng.com.gzl0ng.apitest.beans.SensoReading;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author 郭正龙
 * @date 2021-11-03
 */
public class SinkTest3_Es {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.readTextFile("E:\\IntelliJ_work\\Flinktutorial\\src\\main\\resources\\sensor.txt");
        DataStream<SensoReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensoReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        //定义es的连接配置
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("localhost",9200));

        dataStream.addSink(new ElasticsearchSink.Builder<SensoReading>(httpHosts,new MyEsSinkFunction()).build());
        env.execute();
    }

    //实现自定义的es写入操作的function
    public  static class MyEsSinkFunction implements ElasticsearchSinkFunction<SensoReading>{

        @Override
        public void process(SensoReading sensoReading, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
            //定义写入的数据source
            HashMap<String, String> dataSource = new HashMap<>();
            dataSource.put("id",sensoReading.getId());
            dataSource.put("temp",sensoReading.getTemperature().toString());
            dataSource.put("ts",sensoReading.getTimestamp().toString());

            //创建请求，作为向es发起的写入命令
            IndexRequest indexRequest = Requests.indexRequest()
                    .index("sensor")
                    .type("readingdata")
                    .source(dataSource);

            //用index发起请求
            requestIndexer.add(indexRequest);
        }
    }
}
