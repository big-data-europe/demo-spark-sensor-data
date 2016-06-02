# demo-spark-sensor-data
Demo Spark application to transform data gathered on sensors for a heatmap application.

## Description
Our demo-case considers the location of trackers at various time intervals. We assume we continuously monitor the location of the tracker and we intend to display a heatmap which shows where the trackers were at various time intervals. Given that this is a Big Data project, we envision having received the location of many thousands of trackers. The output will display a heatmap indicating how busy an area was at a time-interval.

![Demo output visualization](http://i.imgsafe.org/ff58a9e83e.png)

The algorithm assumes that the locations of the trackers have been received through an input pipeline and that they are available in CSV format on HDFS. The algorithm will output JSON content which can be used to render the heatmap on a web-capable devices. As this could comprise of billions of events, the calculation will occur offline. The output of our calculation should be small enough so that it can be sent to a web browser for final display without major modifications. This entails minimizing the amount of content in our target output so that it's both small enough and still usable.

In our setting we receive a large set of data inputs in a simple format and are required to output the contents in a more condense format. The condense format will allow us to display a heatmap indicating the activity in various regions.

## Demo flow
Running the demo consists of the following steps:

1. Setup HDFS
2. Setup Spark cluster
3. Put input CSV file on HDFS
4. Compute aggregations
5. Get output from HDFS

The pipeline is described in [pipeline.ttl](https://github.com/big-data-europe/demo-spark-sensor-data/blob/master/data/db/toLoad/pipeline.ttl) which is an export of the [Pipeline Builder application](https://github.com/big-data-europe/app-pipeline-builder). The components that are part of the demo are listed in [docker-compose.yml](https://github.com/big-data-europe/demo-spark-sensor-data/blob/master/docker-compose.yml) together with their configuration. The [docker-compose.yml](https://github.com/big-data-europe/demo-spark-sensor-data/blob/master/docker-compose.yml) also contains the BDE support layer as provided by [BDE Pipeline application](https://github.com/big-data-europe/app-bde-pipeline).

## Configuration
### Aggregation computation in Spark
The application requires 4 arguments. They can be overwritten at runtime by passing them as environment variables to the demo component in `docker-compose.yml`:

1. `APP_ARGS_OWNER`: owner (default: `localhost`)
2. `APP_ARGS_MAX_DETAIL`: maximum level of refinement of the grid (default: `128`)
3. `APP_ARGS_INPUT`: path to the input folder on HDFS containing the sensor data as CSV (default: `/input`)
4. `APP_ARGS_OUTPUT`: path to the output folder on HDFS to write the resulting JSON to (default: `/output`)

The input file (`localhost.csv` by default) must be available on HDFS in the configured input folder (`APP_ARGS_INPUT`).

### Application pipeline in Docker Compose
Update the DNS names in [the Integrator UI configuration](https://github.com/big-data-europe/demo-spark-sensor-data/tree/master/config/integrator) and [docker-compose.yml](https://github.com/big-data-europe/demo-spark-sensor-data/blob/master/docker-compose.yml) with the DNS name on which you will host the demo application. If you won't host the demo application publicly, you can also make the currently configured DNS name `http://demo.big-data-europe.local` point to the frontend host in your cluster by editing your local `/etc/hosts` file. The frontend host is the node in your cluster on which the proxy container runs (see below).

Make sure the mounted volumes in [docker-compose.yml](https://github.com/big-data-europe/demo-spark-sensor-data/blob/master/docker-compose.yml) are absolute paths which are available on each node in your cluster and contain the correct configuration files. If you don't make use of a distributed volume driver, this means the configuration should be replicated in the same folder on each node.

## Running the demo application
Install the BDE platform as described in [the installation guide on the BDE Wiki](https://github.com/big-data-europe/README/wiki/Installation). You can warm up the cluster by pulling [the required Docker images of the demo](https://github.com/big-data-europe/demo-spark-sensor-data/blob/master/docker-compose.yml) on each node.

To run the demo application as a BDE pipeline, execute the following commands:
```
git clone https://github.com/big-data-europe/demo-spark-sensor-data.git
cd demo-spark-sensor-data
docker-compose up -d  ## Send this to the Swarm Manager
```

Next, start a proxy service (if this is not running already) on the frontend host:
```
docker run -p 80:80 --name=proxy --net=demosparksensordata_default -e DOCKER_HOST=tcp://{swarm-manager-host}:{swarm-manager-port} -d rckrdstrgrd/nginx-proxy
```

The integrator UI should now be available on the demo DNS root.
