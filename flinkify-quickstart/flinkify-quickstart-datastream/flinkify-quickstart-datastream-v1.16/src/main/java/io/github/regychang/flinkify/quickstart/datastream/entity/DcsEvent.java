package io.github.regychang.flinkify.quickstart.datastream.entity;

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

    public Integer salt;

    public boolean hasBeenJoined;

    private DcsEvent(
            String serialNumber,
            String modelName,
            String moNumber,
            String side,
            LocalDateTime startedAt,
            LocalDateTime endedAt,
            LocalDateTime createdAt,
            String dcsId,
            String wsId,
            String sensorMapObjectId,
            Integer salt) {
        this.serialNumber = serialNumber;
        this.modelName = modelName;
        this.moNumber = moNumber;
        this.side = side;
        this.startedAt = startedAt;
        this.endedAt = endedAt;
        this.createdAt = createdAt;
        this.dcsId = dcsId;
        this.wsId = wsId;
        this.sensorMapObjectId = sensorMapObjectId;
        this.salt = salt;
    }
}
