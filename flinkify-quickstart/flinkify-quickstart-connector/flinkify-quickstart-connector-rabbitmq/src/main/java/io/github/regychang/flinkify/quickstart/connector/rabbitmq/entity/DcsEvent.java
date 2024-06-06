package io.github.regychang.flinkify.quickstart.connector.rabbitmq.entity;

import com.alibaba.fastjson2.annotation.JSONField;
import lombok.Data;

import java.time.LocalDateTime;

@Data
public class DcsEvent {

    @JSONField(name = "serial_number")
    public String serialNumber;

    @JSONField(name = "model_name")
    public String modelName;

    @JSONField(name = "mo_number", alternateNames = "moNumber")
    public String moNumber;

    @JSONField(name = "side")
    public String side;

    @JSONField(name = "started_at")
    public LocalDateTime startedAt;

    @JSONField(name = "ended_at")
    public LocalDateTime endedAt;

    @JSONField(name = "created_at")
    public LocalDateTime createdAt;

    @JSONField(name = "station_number")
    public String dcsId;

    @JSONField(name = "work_station_number")
    public String wsId;

    @JSONField(name = "sensor_map_id")
    public String sensorMapObjectId;

    public static class FromRabbitmq {
    }
}
