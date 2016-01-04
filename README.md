# demo-spark-sensor-data
Demo Spark application to transform data gathered on sensors for a heatmap application.

## Description
Our demo-case considers the location of trackers at various time intervals. We assume we continuously monitor the location of the tracker and we intend to display a heatmap which shows where the trackers were at various time intervals. Given that this is a Big Data project, we envision having received the location of many thousands of trackers. The output will display a heatmap indicating how busy an area was at a time-interval.

The algorithm assumes that the contents of the trackers has been received through an input pipeline and that it's available in CSV format. The algorithm will output JSON content which can be used to render the heatmap on a web-capable devices. As this could comprise of billions of events, the calculation will occur offline. The output of our calculation should be small enough so that it can be sent to a web browser for final display without major modifications. This entails minimizing the amount of content in our target output so that it's both small enough and still usable.

In our setting we receive a large set of data inputs in a simple format and are required to output the contents in a more condense format. The condense format will allow us to display a heatmap indicating the activity in various regions.

## Spark main application class
The main application class is `com.tenforce.bde.spark.demo.sensors.Application`.
The application requires as application arguments:

1. owner (default: `localhost`)
2. path to the input folder containing the sensor data as CSV (default: `/data/input`)
3. path to the output folder to write the resulting JSON to (default: `/data/output`)

All Spark workers should have access to the `/data/input` and `/data/output` directories.

## Running the application on a Spark standalone cluster

To run the application on a standalone Spark cluster

1. Setup a Spark cluster as described on http://github.com/big-data-europe/docker-spark
2. Build the Docker image: `docker build --rm=true -t bde/spark-demo .`
3. Run the Docker container: `docker run --link spark-master:spark-master -d bde/spark-demo`

The application runtime arguments can be configured by setting the `APP_ARGS_OWNER`, `APP_ARGS_INPUT` and `APP_ARGS_OUTPUT` environment variables at container runtime. E.g.  `docker run --link spark-master:spark-master -e APP_ARGS_OWNER=foo -d bde/spark-demo`
