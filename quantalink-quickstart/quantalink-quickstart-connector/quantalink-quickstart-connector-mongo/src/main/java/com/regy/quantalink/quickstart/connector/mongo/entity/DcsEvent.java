package com.regy.quantalink.quickstart.connector.mongo.entity;

import com.alibaba.fastjson2.annotation.JSONField;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.time.LocalDateTime;

/**
 * @author regy
 */
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DcsEvent event = (DcsEvent) o;

        return new EqualsBuilder().append(serialNumber, event.serialNumber).append(modelName, event.modelName).append(moNumber, event.moNumber).append(side, event.side).append(startedAt, event.startedAt).append(endedAt, event.endedAt).append(createdAt, event.createdAt).append(dcsId, event.dcsId).append(wsId, event.wsId).append(sensorMapObjectId, event.sensorMapObjectId).isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).append(serialNumber).append(modelName).append(moNumber).append(side).append(startedAt).append(endedAt).append(createdAt).append(dcsId).append(wsId).append(sensorMapObjectId).toHashCode();
    }

    @Override
    public String toString() {
        return "DcsEvent{" +
                "serialNumber='" + serialNumber + '\'' +
                ", modelName='" + modelName + '\'' +
                ", moNumber='" + moNumber + '\'' +
                ", side='" + side + '\'' +
                ", startedAt=" + startedAt +
                ", endedAt=" + endedAt +
                ", createdAt=" + createdAt +
                ", dcsId='" + dcsId + '\'' +
                ", wsId='" + wsId + '\'' +
                ", sensorMapObjectId='" + sensorMapObjectId + '\'' +
                '}';
    }
}
