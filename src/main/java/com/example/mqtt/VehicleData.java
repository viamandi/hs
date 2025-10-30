package com.example.mqtt;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

@Entity
@Table(name = "vehicle_data")
public class VehicleData {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String operationId;
    private String vin;
    private Float loDegree;
    private String loDirection;
    private Float laDegree;
    private String laDirection;
    private Instant lastUpdateTimestamp;

    // Getters and Setters

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getOperationId() {
        return operationId;
    }

    public void setOperationId(String operationId) {
        this.operationId = operationId;
    }

    public String getVin() {
        return vin;
    }

    public void setVin(String vin) {
        this.vin = vin;
    }

    public Float getLoDegree() {
        return loDegree;
    }

    public void setLoDegree(Float loDegree) {
        this.loDegree = loDegree;
    }

    public String getLoDirection() {
        return loDirection;
    }

    public void setLoDirection(String loDirection) {
        this.loDirection = loDirection;
    }

    public Float getLaDegree() {
        return laDegree;
    }

    public void setLaDegree(Float laDegree) {
        this.laDegree = laDegree;
    }

    public String getLaDirection() {
        return laDirection;
    }

    public void setLaDirection(String laDirection) {
        this.laDirection = laDirection;
    }

    public Instant getLastUpdateTimestamp() {
        return lastUpdateTimestamp;
    }

    public void setLastUpdateTimestamp(Instant lastUpdateTimestamp) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
    }

    @Override
    public String toString() {
        return "VehicleData{" +
                "id=" + id +
                ", operationId='" + operationId + '\'' +
                ", vin='" + vin + '\'' +
                ", loDegree=" + loDegree +
                ", loDirection='" + loDirection + '\'' +
                ", laDegree=" + laDegree +
                ", laDirection='" + laDirection + '\'' +
                ", lastUpdateTimestamp=" + lastUpdateTimestamp +
                '}';
    }
}