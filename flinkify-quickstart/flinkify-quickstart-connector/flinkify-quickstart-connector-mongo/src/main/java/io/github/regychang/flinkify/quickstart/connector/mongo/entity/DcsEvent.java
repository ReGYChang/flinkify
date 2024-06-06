package io.github.regychang.flinkify.quickstart.connector.mongo.entity;

import com.alibaba.fastjson2.annotation.JSONField;
import lombok.Data;

import java.time.LocalDateTime;

@Data
public class DcsEvent {

    @JSONField(name = "serial_number")
    private String serialNumber;

    @JSONField(name = "model_name")
    private String modelName;

    @JSONField(name = "mo_number", alternateNames = "moNumber")
    private String moNumber;

    @JSONField(name = "side")
    private String side;

    @JSONField(name = "started_at")
    private LocalDateTime startedAt;

    @JSONField(name = "ended_at")
    private LocalDateTime endedAt;

    @JSONField(name = "created_at")
    private LocalDateTime createdAt;

    @JSONField(name = "station_number")
    private String dcsId;

    @JSONField(name = "work_station_number")
    private String wsId;

    @JSONField(name = "sensor_map_id")
    private String sensorMapObjectId;
}
