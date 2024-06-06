package io.github.regychang.flinkify.quickstart.connector.nebula.entity;

import com.alibaba.fastjson2.annotation.JSONField;

import java.time.LocalDateTime;
import java.util.List;

public class SensorMap {

    @JSONField(name = "_id")
    public String oid;

    @JSONField(name = "created_at")
    public LocalDateTime createdAt;

    @JSONField(name = "updated_at")
    public LocalDateTime updateAt;

    @JSONField(name = "production_lines")
    public List<ProductionLine> productionLines;

    public SensorMap() {
    }

    public static class ProductionLine {
        @JSONField(name = "_id")
        public String oid;

        @JSONField(name = "id")
        public String lineId;

        @JSONField(name = "name")
        public String name;

        @JSONField(name = "description")
        public String description;

        @JSONField(name = "sequence")
        public List<SequenceInfo> sequence;

        @JSONField(name = "workstations")
        public List<Workstation> workstations;

        @JSONField(name = "created_at")
        public LocalDateTime createdAt;

        @JSONField(name = "updated_at")
        public LocalDateTime updatedAt;

        public ProductionLine() {
        }
    }

    public static class SequenceInfo {
        @JSONField(name = "workstation_id")
        public String workstationOid;

        @JSONField(name = "base_id")
        public String baseOid;

        @JSONField(name = "offset")
        public Double offset;

        @JSONField(name = "processing_time")
        public Double processingTime;

        public SequenceInfo() {
        }
    }

    public static class Workstation {
        @JSONField(name = "_id")
        public String oid;

        @JSONField(name = "id")
        public String stationId;

        @JSONField(name = "name")
        public String name;

        @JSONField(name = "station_type")
        public String stationType;

        @JSONField(name = "description")
        public String description;

        @JSONField(name = "tag_ids")
        public List<String> tagIds;

        @JSONField(name = "tags")
        public List<Tag> tags;

        @JSONField(name = "camera_ids")
        public String cameraIds;

        @JSONField(name = "created_at")
        public LocalDateTime createdAt;

        @JSONField(name = "updated_at")
        public LocalDateTime updatedAt;

        public Workstation() {
        }
    }

    public static class Tag {
        @JSONField(name = "_id")
        public String oid;

        @JSONField(name = "id")
        public String tagId;

        @JSONField(name = "group_name")
        public String groupName;

        @JSONField(name = "tag_type")
        public String tagType;

        @JSONField(name = "description")
        public String description;

        @JSONField(name = "unit")
        public String unit;

        @JSONField(name = "high_limit")
        public Double highLimit;

        @JSONField(name = "low_limit")
        public Double lowLimit;

        @JSONField(name = "created_at")
        public LocalDateTime createdAt;

        @JSONField(name = "updated_at")
        public LocalDateTime updatedAt;

        public Tag() {
        }
    }
}
