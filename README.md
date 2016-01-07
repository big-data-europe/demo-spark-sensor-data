# demo-spark-sensor-data
Demo Spark application to transform data gathered on sensors for a heatmap application.

## Description
Our demo-case considers the location of trackers at various time intervals. We assume we continuously monitor the location of the tracker and we intend to display a heatmap which shows where the trackers were at various time intervals. Given that this is a Big Data project, we envision having received the location of many thousands of trackers. The output will display a heatmap indicating how busy an area was at a time-interval.

The algorithm assumes that the contents of the trackers has been received through an input pipeline and that it's available in CSV format on HDFS. The algorithm will output JSON content which can be used to render the heatmap on a web-capable devices. As this could comprise of billions of events, the calculation will occur offline. The output of our calculation should be small enough so that it can be sent to a web browser for final display without major modifications. This entails minimizing the amount of content in our target output so that it's both small enough and still usable.

In our setting we receive a large set of data inputs in a simple format and are required to output the contents in a more condense format. The condense format will allow us to display a heatmap indicating the activity in various regions.

## Running the demo application
The application requires HDFS and a Spark cluster. To run the application, execute the following steps:

1. Setup a single node HDFS in a Docker container by running `dr run -it --name hdfs sequenceiq/hadoop-docker:2.6.0 /etc/bootstrap.sh -bash`
2. Setup a Spark cluster as described on http://github.com/big-data-europe/docker-spark. Make sure the Spark workers have a link to HDFS.
3. Build the Docker image: `docker build --rm=true -t bde/spark-demo .`
4. Run the Docker container: `docker run --link spark-master:spark-master --link hdfs:hdfs -d bde/spark-demo`

The application requires 4 arguments. They can be overwritten at runtime by passing them as environment variables:

1. APP_ARGS_OWNER: owner (default: `localhost`)
2. APP_ARGS_MAX_DETAIL: maximum level of refinement of the grid (default: `128`)
3. APP_ARGS_INPUT: path to the input folder on HDFS containing the sensor data as CSV (default: `/input`)
4. APP_ARGS_OUTPUT: path to the output folder on HDFS to write the resulting JSON to (default: `/output`)

The HDFS namenode location can be configured through the environment variable `HDFS_URL` which is set to `hdfs://hdfs:9000` by default. All Spark workers should have access to HDFS with this same URL, so make sure to link the HDFS container with the correct alias to the Spark worker containers.

The input file (`localhost.csv` by default) must be available on HDFS in the configured input folder (`APP_ARGS_INPUT`).

