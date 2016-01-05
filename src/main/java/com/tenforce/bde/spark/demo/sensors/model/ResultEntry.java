package com.tenforce.bde.spark.demo.sensors.model;

public class ResultEntry {

  private double latitude;

  private double longitude;

  private int count;

  public ResultEntry(double latitude, double longitude, int count) {
    this.latitude = latitude;
    this.longitude = longitude;
    this.count = count;
  }

  public double getLatitude() {
    return latitude;
  }

  public void setLatitude(double latitude) {
    this.latitude = latitude;
  }

  public double getLongitude() {
    return longitude;
  }

  public void setLongitude(double longitude) {
    this.longitude = longitude;
  }

  public int getCount() {
    return count;
  }

  public void setCount(int count) {
    this.count = count;
  }
}
