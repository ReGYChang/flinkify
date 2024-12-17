package io.github.regychang.flinkify.quickstart.connector.sqlserver.sink;

import io.github.regychang.flinkify.flink.core.utils.jdbc.Column;
import io.github.regychang.java.faker.annotation.JFaker;
import java.sql.JDBCType;
import java.time.LocalDateTime;
import lombok.Data;

@Data
public class IPBDHU {

    @Column(name = "PatientID", type = JDBCType.VARCHAR)
    @JFaker(key = "PatientID", length = 15)
    private String patientID;

    @Column(name = "AdmissionDate", type = JDBCType.TIMESTAMP)
    @JFaker(
            key = "AdmissionDate",
            features = {JFaker.Feature.LinearTimestamp})
    private LocalDateTime admissionDate;

    @Column(name = "DischargeDate", type = JDBCType.TIMESTAMP)
    @JFaker(
            key = "DischargeDate",
            features = {JFaker.Feature.LinearTimestamp})
    private LocalDateTime dischargeDate;

    @Column(name = "WardID", type = JDBCType.VARCHAR)
    @JFaker(key = "WardID", format = "W[A-Z][0-9]{3}", length = 5)
    private String wardID;

    @Column(name = "DoctorID", type = JDBCType.VARCHAR)
    @JFaker(key = "DoctorID", format = "D[0-9]{4}", length = 5)
    private String doctorID;

    @Column(name = "PatientName", type = JDBCType.NVARCHAR)
    @JFaker(key = "PatientName")
    private String patientName;

    @Column(name = "Age", type = JDBCType.INTEGER)
    @JFaker(key = "Age", min = 0, max = 120)
    private int age;

    @Column(name = "Gender", type = JDBCType.CHAR)
    @JFaker(
            key = "Gender",
            values = {"M", "F"})
    private char gender;

    @Column(name = "AdmissionType", type = JDBCType.VARCHAR)
    @JFaker(
            key = "AdmissionType",
            values = {"Emergency", "Planned", "Transfer", "Outpatient"})
    private String admissionType;

    @Column(name = "CreatedAt", type = JDBCType.TIMESTAMP)
    @JFaker(
            key = "CreatedAt",
            features = {JFaker.Feature.LinearTimestamp})
    private LocalDateTime createdAt;
}
