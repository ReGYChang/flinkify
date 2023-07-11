package com.regy.quantalink.quickstart.connector.mongo.entity;

import com.alibaba.fastjson2.annotation.JSONField;

import java.time.LocalDateTime;

/**
 * @author regy
 */
public class Record {

    @JSONField(name = "serial_number")
    public String serialNumber;
    @JSONField(name = "started_at")
    public LocalDateTime startedAt;
    @JSONField(name = "ended_at")
    public LocalDateTime endedAt;
    @JSONField(name = "production_line_id")
    public String lineId;
    @JSONField(name = "production_line_name")
    public String lineName;

    public Record(String serialNumber, LocalDateTime startedAt, LocalDateTime endedAt, String lineId, String lineName) {
        this.serialNumber = serialNumber;
        this.startedAt = startedAt;
        this.endedAt = endedAt;
        this.lineId = lineId;
        this.lineName = lineName;
    }

    @Override
    public String toString() {
        return "Record{" +
                "serialNumber='" + serialNumber + '\'' +
                ", startedAt=" + startedAt +
                ", endedAt=" + endedAt +
                ", lineId='" + lineId + '\'' +
                ", lineName='" + lineName + '\'' +
                '}';
    }
}
