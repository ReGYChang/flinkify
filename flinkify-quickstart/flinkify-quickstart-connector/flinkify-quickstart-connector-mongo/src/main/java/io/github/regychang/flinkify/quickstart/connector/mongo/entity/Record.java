package io.github.regychang.flinkify.quickstart.connector.mongo.entity;

import com.alibaba.fastjson2.annotation.JSONField;
import lombok.Data;

import java.time.LocalDateTime;

@Data
public class Record {

    @JSONField(name = "serial_number")
    private String serialNumber;

    @JSONField(name = "started_at")
    private LocalDateTime startedAt;

    @JSONField(name = "ended_at")
    private LocalDateTime endedAt;

    @JSONField(name = "production_line_id")
    private String lineId;

    @JSONField(name = "production_line_name")
    private String lineName;

    public Record(
            String serialNumber, LocalDateTime startedAt, LocalDateTime endedAt, String lineId, String lineName) {
        this.serialNumber = serialNumber;
        this.startedAt = startedAt;
        this.endedAt = endedAt;
        this.lineId = lineId;
        this.lineName = lineName;
    }
}
