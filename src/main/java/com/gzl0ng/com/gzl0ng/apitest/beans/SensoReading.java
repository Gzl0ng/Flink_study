package com.gzl0ng.com.gzl0ng.apitest.beans;/**
 *
 * @author 郭正龙
 * @date 2021-11-01
 */

//传感器温度度数的数据类型
public class SensoReading {
    //属性：id 时间戳，温度值
    private String id;
    private Long timestamp;
    private Double temperature;

    public SensoReading() {
    }

    public SensoReading(String id, Long timestamp, Double temperature) {
        this.id = id;
        this.timestamp = timestamp;
        this.temperature = temperature;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Double getTemperature() {
        return temperature;
    }

    public void setTemperature(Double temperature) {
        this.temperature = temperature;
    }

    @Override
    public String toString() {
        return "SensoReading{" +
                "id='" + id + '\'' +
                ", timestamp=" + timestamp +
                ", temperature=" + temperature +
                '}';
    }
}
