package com.tenforce.bde.spark.demo.sensors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tenforce.bde.spark.demo.sensors.model.Coordinate;
import com.tenforce.bde.spark.demo.sensors.model.Measurement;
import com.tenforce.bde.spark.demo.sensors.utils.TimestampComparator;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class Application {

  private static int MAX_DETAIL = 128;
//  private static String SPARK_MASTER = "local[4]";
  private static String SPARK_MASTER = "spark://spark-master:7077";
  private static ObjectMapper MAPPER = new ObjectMapper();

  public static void main(String[] args) throws IOException {
    if(args.length < 3) {
      throw new IllegalArgumentException("Owner, input and output folder must be passed as arguments");
    }
    String owner = args[0];
    String csvFile = new File(args[1], owner + ".csv").getAbsolutePath();
    String outputPath = new File(args[2], owner).getAbsolutePath();

    FileUtils.deleteDirectory(new File(outputPath));

    SparkConf sparkConf = new SparkConf().setAppName("BDE-SensorDemo").setMaster(SPARK_MASTER);
    JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

    JavaRDD<Measurement> measurements = csvToMeasurements(sparkContext, csvFile);
    JavaPairRDD<Coordinate, Measurement> measurementsByCoordinate = mapToGridBox(measurements);

    LocalDateTime minTimestamp = measurements.min(new TimestampComparator()).getTimestamp();
    LocalDateTime maxTimestamp = measurements.max(new TimestampComparator()).getTimestamp();
    long duration = minTimestamp.until(maxTimestamp, ChronoUnit.MILLIS);

    for(int detail = 1; detail <= MAX_DETAIL; detail *= 2) {
      long timeStep = duration / detail;
      File detailPath = new File(outputPath + File.separator + detail);
      FileUtils.forceMkdir(detailPath);

      for(int i = 0; i < detail; i++) {
        LocalDateTime start = minTimestamp.plus(timeStep * i, ChronoUnit.MILLIS);
        LocalDateTime end = minTimestamp.plus(timeStep * (i+1), ChronoUnit.MILLIS);
        Map<Coordinate, Object> result = countPerGridBoxInTimePeriod(measurementsByCoordinate, start, end);

        File outputFile = new File(detailPath, (i+1) + ".json");
        writeOutputToFile(result, outputFile);
      }
    }

    sparkContext.close();
    sparkContext.stop();
  }

  /**
   * Converts each row from the CSV file to a Measurement
   *
   * @param javaContext | Java Spark context
   * @param csvFile     | Path to the CSV file containing the sensor data
   * @return A JavaRDD containing all data from the CSV file as Measurements
   */
  private static JavaRDD<Measurement> csvToMeasurements(JavaSparkContext javaContext, String csvFile) {
    SQLContext sqlContext = new SQLContext(javaContext);
    DataFrame dataFrame = sqlContext.read().format("com.databricks.spark.csv").option("header", "true").load(csvFile);

    return dataFrame.javaRDD().map(
      new Function<Row, Measurement>() {
        @Override
        public Measurement call(Row row) throws Exception {
          LocalDateTime time = LocalDateTime.parse(row.getString(row.fieldIndex("timestamp")), DateTimeFormatter.ISO_DATE_TIME);
          Double latitude = Double.parseDouble(row.getString(row.fieldIndex("latitude")));
          Double longitude = Double.parseDouble(row.getString(row.fieldIndex("longitude")));
          Coordinate coordinate = new Coordinate(latitude, longitude);
          return new Measurement(coordinate, time);
        }
      }
    );
  }

  /**
   * Maps the measurements by rounding the coordinate.
   * The world is defined by a grid of boxes, each box has a size of 0.0005 by 0.0005.
   * Every mapping will be rounded to the center of the box it is part of.
   * Boundary cases will be rounded up, so a coordinate on (-0.00025,0) will be rounded to (0,0),
   * while the coordinate (0.00025,0) will be rounded to (0.0005,0).
   *
   * @param measurements | The dataset of measurements
   * @return A Map linking rounded coordinates with a measurement in the original dataset
   */
  private static JavaPairRDD<Coordinate, Measurement> mapToGridBox(JavaRDD<Measurement> measurements) {
    return measurements.mapToPair(
      new PairFunction<Measurement, Coordinate, Measurement>() {
        @Override
        public Tuple2<Coordinate, Measurement> call(Measurement measurement) throws Exception {
          double mappedLatitude = (double) (5 * Math.round((measurement.getCoordinate().getLatitude() * 10000) / 5)) / 10000;
          double mappedLongitude = (double) (5 * Math.round((measurement.getCoordinate().getLongitude() * 10000) / 5)) / 10000;
          Coordinate mappedCoordinate = new Coordinate(mappedLatitude, mappedLongitude);

          return new Tuple2<>(mappedCoordinate, measurement);
        }
      }
    );
  }

  /**
   * Reduces the dataset by counting the number of measurements for a specific grid box in a specific time period
   *
   * @param measurementsByCoordinate | Map of all measurements linking the rounded coordinates to the original measurement
   * @param start | Start of the time period
   * @param end   | End of the time period
   * @return A map linking grid boxes to the number of measurements in that box in a specific time period
   */
  private static Map<Coordinate, Object> countPerGridBoxInTimePeriod(JavaPairRDD<Coordinate, Measurement> measurementsByCoordinate, LocalDateTime start, LocalDateTime end) {
    return measurementsByCoordinate.filter(
      new Function<Tuple2<Coordinate, Measurement>, Boolean>() {
        @Override
        public Boolean call(Tuple2<Coordinate, Measurement> tuple) throws Exception {
          Measurement measurement = tuple._2();
          return (measurement.getTimestamp().isEqual(start) || measurement.getTimestamp().isAfter(start))
            && measurement.getTimestamp().isBefore(end);
        }
      }
    ).countByKey();
  }

  /**
   * Writes the count per grid box for a specific time period to a JSON file
   * @param countPerGridBox | Map containing the count of measurements per grid box
   * @param outputFile      | File to write the JSON output
   * @throws IOException
   */
  private static void writeOutputToFile(Map<Coordinate, Object> countPerGridBox, File outputFile) throws IOException {
    List<Map<String, Object>> gridBoxes = new ArrayList<>();
    for (Coordinate coordinate : countPerGridBox.keySet()) {
      Map<String, Object> gridBox = new HashMap<>();
      gridBox.put("latitude", coordinate.getLatitude());
      gridBox.put("longitude", coordinate.getLongitude());
      gridBox.put("count", countPerGridBox.get(coordinate));
      gridBoxes.add(gridBox);
    }
    Map<String, Object> data = new HashMap<>();
    data.put("data", gridBoxes);
    MAPPER.writeValue(outputFile, data);
  }
}
