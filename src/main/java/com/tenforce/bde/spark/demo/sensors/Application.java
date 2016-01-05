package com.tenforce.bde.spark.demo.sensors;

import com.tenforce.bde.spark.demo.sensors.model.Coordinate;
import com.tenforce.bde.spark.demo.sensors.model.Measurement;
import com.tenforce.bde.spark.demo.sensors.model.ResultEntry;
import com.tenforce.bde.spark.demo.sensors.utils.TimestampComparator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import scala.Tuple2;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

public class Application {

  private static int MAX_DETAIL = 128;
//  private static String SPARK_MASTER = "local[4]";
  private static String SPARK_MASTER = "spark://spark-master:7077";

  public static void main(String[] args) throws IOException {
    if(args.length < 3) {
      throw new IllegalArgumentException("Owner, input and output folder must be passed as arguments");
    }
    String owner = args[0];
    String csvFile = args[1] + "/" + owner + ".csv";
    String outputPath = args[2] + "/" + owner;

    SparkConf sparkConf = new SparkConf().setAppName("BDE-SensorDemo").setMaster(SPARK_MASTER);
    JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
    SQLContext sqlContext = new SQLContext(sparkContext);

    JavaRDD<Measurement> measurements = csvToMeasurements(sqlContext, csvFile);
    JavaRDD<Measurement> measurementsWithRoundedCoordinates = roundCoordinates(measurements);

    LocalDateTime minTimestamp = measurements.min(new TimestampComparator()).getTimestamp();
    LocalDateTime maxTimestamp = measurements.max(new TimestampComparator()).getTimestamp();
    long duration = minTimestamp.until(maxTimestamp, ChronoUnit.MILLIS);

    for(int detail = 1; detail <= MAX_DETAIL; detail *= 2) {
      long timeStep = duration / detail;
      String detailPath = outputPath + "/" + detail;

      for(int i = 0; i < detail; i++) {
        LocalDateTime start = minTimestamp.plus(timeStep * i, ChronoUnit.MILLIS);
        LocalDateTime end = minTimestamp.plus(timeStep * (i+1), ChronoUnit.MILLIS);
        JavaRDD<Measurement> measurementsFilteredByTime = filterByTime(measurementsWithRoundedCoordinates, start, end);
        JavaPairRDD<Coordinate, Integer> counts = countPerGridBox(measurementsFilteredByTime);
        JavaRDD<ResultEntry> results = convertToResult(counts);

        String fileName = detailPath + "/" + (i+1);
        sqlContext.createDataFrame(results, ResultEntry.class).write().mode(SaveMode.Overwrite).json(fileName);
      }
    }

    sparkContext.close();
    sparkContext.stop();
  }

  /**
   * Converts each row from the CSV file to a Measurement
   *
   * @param sqlContext  | Spark SQL context
   * @param csvFile     | Path to the CSV file containing the sensor data
   * @return A set containing all data from the CSV file as Measurements
   */
  private static JavaRDD<Measurement> csvToMeasurements(SQLContext sqlContext, String csvFile) {
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
   * @return A set of measurements with rounded coordinates
   */
  private static JavaRDD<Measurement> roundCoordinates(JavaRDD<Measurement> measurements) {
    return measurements.map(
      new Function<Measurement, Measurement>() {
        @Override
        public Measurement call(Measurement measurement) throws Exception {
          double roundedLatitude = (double) (5 * Math.round((measurement.getCoordinate().getLatitude() * 10000) / 5)) / 10000;
          double roundedLongitude = (double) (5 * Math.round((measurement.getCoordinate().getLongitude() * 10000) / 5)) / 10000;
          Coordinate roundedCoordinate = new Coordinate(roundedLatitude, roundedLongitude);
          measurement.setRoundedCoordinate(roundedCoordinate);
          return measurement;
        }
      }
    );
  }

  /**
   * Filter the measurements in a given time period
   *
   * @param measurements | The dataset of measurements
   * @param start | Start of the time period
   * @param end   | End of the time period
   * @return A set of measurements in the given time period
   */
  private static JavaRDD<Measurement> filterByTime(JavaRDD<Measurement> measurements, LocalDateTime start, LocalDateTime end) {
    return measurements.filter(
      new Function<Measurement, Boolean>() {
        @Override
        public Boolean call(Measurement measurement) throws Exception {
          return (measurement.getTimestamp().isEqual(start) || measurement.getTimestamp().isAfter(start))
            && measurement.getTimestamp().isBefore(end);
        }
      }
    );
  }

  /**
   * Reduces the dataset by counting the number of measurements for a specific grid box (rounded coordinate)
   *
   * @param measurements | The dataset of measurements
   * @return A set of tuples linking rounded coordinates to their number of occurrences
   */
  private static JavaPairRDD<Coordinate, Integer> countPerGridBox(JavaRDD<Measurement> measurements) {
    return measurements.mapToPair(
      new PairFunction<Measurement, Coordinate, Integer>() {
        @Override
        public Tuple2<Coordinate, Integer> call(Measurement measurement) throws Exception {
          return new Tuple2<Coordinate, Integer>(measurement.getRoundedCoordinate(), 1);
        }
      }
    ).reduceByKey(
      new Function2<Integer, Integer, Integer>() {
        @Override
        public Integer call(Integer a, Integer b) throws Exception {
          return a + b;
        }
      }
    );
  }

  /**
   * Maps the tuples of coordinates and counts to serializable result objects
   *
   * @param tuples | Set of tuples linking coordinates to a count
   * @return A set of serializable result objects
   */
  private static JavaRDD<ResultEntry> convertToResult(JavaPairRDD<Coordinate, Integer> tuples) {
    return tuples.map(
      new Function<Tuple2<Coordinate, Integer>, ResultEntry>() {
        @Override
        public ResultEntry call(Tuple2<Coordinate, Integer> tuple) throws Exception {
          Coordinate coordinate = tuple._1();
          return new ResultEntry(coordinate.getLatitude(), coordinate.getLongitude(), tuple._2());
        }
      }
    );
  }

}
