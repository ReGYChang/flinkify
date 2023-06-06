package com.regy.quantalink.quickstart.datastream.entity;

import com.alibaba.fastjson2.annotation.JSONField;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
    public Integer salt;
    public boolean hasBeenJoined;

    private DcsEvent(String serialNumber, String modelName, String moNumber, String side, LocalDateTime startedAt, LocalDateTime endedAt, LocalDateTime createdAt, String dcsId, String wsId, String sensorMapObjectId, Integer salt) {
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

    public DcsEvent() {
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DcsEvent dcsEvent = (DcsEvent) o;

        return new EqualsBuilder().append(serialNumber, dcsEvent.serialNumber).append(modelName, dcsEvent.modelName).append(moNumber, dcsEvent.moNumber).append(side, dcsEvent.side).append(startedAt, dcsEvent.startedAt).append(endedAt, dcsEvent.endedAt).append(createdAt, dcsEvent.createdAt).append(dcsId, dcsEvent.dcsId).append(wsId, dcsEvent.wsId).append(sensorMapObjectId, dcsEvent.sensorMapObjectId).append(salt, dcsEvent.salt).isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).append(serialNumber).append(modelName).append(moNumber).append(side).append(startedAt).append(endedAt).append(createdAt).append(dcsId).append(wsId).append(sensorMapObjectId).append(salt).toHashCode();
    }

    public String getSerialNumber() {
        return serialNumber;
    }

    public String getModelName() {
        return modelName;
    }

    public String getMoNumber() {
        return moNumber;
    }

    public String getSide() {
        return side;
    }

    public LocalDateTime getStartedAt() {
        return startedAt;
    }

    public LocalDateTime getEndedAt() {
        return endedAt;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public String getDcsId() {
        return dcsId;
    }

    public String getWsId() {
        return wsId;
    }

    public String getSensorMapObjectId() {
        return sensorMapObjectId;
    }

    @JSONField(serialize = false)
    public Integer getSalt() {
        return salt;
    }

    @JSONField(serialize = false)
    public boolean isHasBeenJoined() {
        return hasBeenJoined;
    }

    public void setHasBeenJoined(boolean hasBeenJoined) {
        this.hasBeenJoined = hasBeenJoined;
    }

    public static class Builder {
        private String serialNumber;
        private String modelName;
        private String moNumber;
        private String side;
        private LocalDateTime startedAt;
        private LocalDateTime endedAt;
        private LocalDateTime createdAt;
        private String dcsId;
        private String wsId;
        private String sensorMapObjectId;
        private Integer salt;

        public Builder withSerialNumber(@Nonnull String serialNumber) {
            this.serialNumber = serialNumber;
            return this;
        }

        public Builder withModelName(@Nonnull String modelName) {
            this.modelName = modelName;
            return this;
        }

        public Builder withMoNumber(@Nonnull String moNumber) {
            this.moNumber = moNumber;
            return this;
        }

        public Builder withSide(@Nonnull String side) {
            this.side = side;
            return this;
        }

        public Builder withStartedAt(@Nonnull LocalDateTime startedAt) {
            this.startedAt = startedAt;
            return this;
        }

        public Builder withEndedAt(@Nonnull LocalDateTime endedAt) {
            this.endedAt = endedAt;
            return this;
        }

        public Builder withCreatedAt(@Nonnull LocalDateTime createdAt) {
            this.createdAt = createdAt;
            return this;
        }

        public Builder withDcsId(@Nonnull String dcsId) {
            this.dcsId = dcsId;
            return this;
        }

        public Builder withWsId(@Nonnull String wsId) {
            this.wsId = wsId;
            return this;
        }

        public Builder withSensorMapObjectId(@Nullable String sensorMapObjectId) {
            this.sensorMapObjectId = sensorMapObjectId;
            return this;
        }

        public Builder withSalt(@Nullable Integer salt) {
            this.salt = salt;
            return this;
        }

        public DcsEvent build() {

            Preconditions.checkNotNull(serialNumber);
            Preconditions.checkNotNull(modelName);
            Preconditions.checkNotNull(moNumber);
            Preconditions.checkNotNull(side);
            Preconditions.checkNotNull(startedAt);
            Preconditions.checkNotNull(endedAt);
            Preconditions.checkNotNull(createdAt);
            Preconditions.checkNotNull(dcsId);
            Preconditions.checkNotNull(wsId);

            return new DcsEvent(serialNumber, modelName, moNumber, side, startedAt,
                    endedAt, createdAt, dcsId, wsId, sensorMapObjectId, salt);
        }
    }
}

