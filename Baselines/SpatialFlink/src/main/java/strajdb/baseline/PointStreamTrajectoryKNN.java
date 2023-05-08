package strajdb.baseline;

import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.LineString;
import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialOperators.QueryConfiguration;
import GeoFlink.spatialOperators.QueryType;
import GeoFlink.spatialOperators.knn.LineStringLineStringKNNQuery;
import GeoFlink.spatialStreams.Deserialization;
import lombok.var;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.kafka.common.protocol.types.Field;
import org.locationtech.jts.geom.Coordinate;
import strajdb.baseline.Operator.MergePointsInWindowToTrajectory;
import strajdb.baseline.Operator.EveryElementTriggers;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

public class PointStreamTrajectoryKNN {
    public static void main(String[] args) throws Exception {
        StopWatch stopwatch = new StopWatch();
        stopwatch.start();
        Configuration config = new Configuration();
        config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
        config.setString(RestOptions.BIND_PORT, "8088");
        config.setString(TaskManagerOptions.TASK_MANAGER_HEAP_MEMORY,"16G");
        config.setInteger(TaskManagerOptions.NUM_TASK_SLOTS,1);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setMaxParallelism(1);
        env.setParallelism(1);

        // parameters
        // Creating uniform grid index by defining its boundaries
        // Tdrive
        double minX = 115.50, maxX = 117.60, minY = 39.60, maxY = 41.10;
        int gridSize = 100;
        UniformGrid uGrid = new UniformGrid(gridSize, minX, maxX, minY, maxY);

        // Porto
//        double minX = -8.7, maxX = -8.5, minY = 41.14, maxY = 41.18;
//        int gridSize = 400;
//        UniformGrid uGrid = new UniformGrid(gridSize, minX, maxX, minY, maxY);

        // Geolife
//        double minX = 108, maxX = 117.60, minY = 22.8, maxY = 41.10;
//        int gridSize = 10000;
//        UniformGrid uGrid = new UniformGrid(gridSize, minX, maxX, minY, maxY);

        //Declaring query variables
        double radius = 0.02;
        int k = 10;

        int allowedLateness = 0;

        /* Windows */
        // total 5 windows
        int windowSize = 5*60; // seconds
        int windowSlideStep = 60; // seconds

        QueryConfiguration windowConf = new QueryConfiguration(QueryType.WindowBased);
        windowConf.setApproximateQuery(false);
        windowConf.setAllowedLateness(allowedLateness);
        windowConf.setWindowSize(windowSize);
        windowConf.setSlideStep(windowSlideStep);

        // First trajectory as query
        // !! change file name for different dataset !!
        String queryFile = "/home/liontao/work/stream-distance/notebooks/preprocess/tdrive-dita-long.txt";
        BufferedReader reader = new BufferedReader(new FileReader(queryFile));
        String firstLine = reader.readLine();
        reader.close();
        var queryCoordinates = new LinkedList<Coordinate>();
        for (String p :
                firstLine.split(";")) {
            var coors = p.split(",");
            queryCoordinates.add(new Coordinate(Double.parseDouble(coors[0]), Double.parseDouble(coors[1])));
        }
        LineString queryLineString = new LineString(queryCoordinates, uGrid);

        // read file as continuous point stream
        var fileStream = env.readTextFile("/home/liontao/work/stream-distance/notebooks/preprocess/tdrive-geoflink-long-small.txt");
        var pointStream = Deserialization.MyTrajectoryStream(fileStream, uGrid)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Point>() {
                    @Override
                    public long extractAscendingTimestamp(Point element) {
                        return element.timeStampMillisec;
                    }
                });

        // group points by ObjID
        var lineStringStream = pointStream.keyBy((KeySelector<Point, String>) point -> point.objID)
                .window(SlidingEventTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSlideStep)))
//                .trigger(new EveryElementTriggers())
                .apply(new MergePointsInWindowToTrajectory(uGrid))
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<LineString>() {
                    @Override
                    public long extractAscendingTimestamp(LineString element) {
                        return element.timeStampMillisec;
                    }
                });

        new LineStringLineStringKNNQuery(windowConf, uGrid).run(lineStringStream, queryLineString, radius, k)
                .map((MapFunction<Tuple3<Long, Long, PriorityQueue<Tuple2<LineString, Double>>>, Tuple3<Long,Integer,String>>) o ->
                        new Tuple3<>(o.f1, o.f2.size(), o.f2.stream().map((s)->s.f0.objID).collect(Collectors.joining(";"))))
                .returns(TupleTypeInfo.getBasicAndBasicValueTupleTypeInfo(Long.class, Integer.class,String.class))
                .print();
//                .writeAsCsv("knn.csv", FileSystem.WriteMode.OVERWRITE,"\n",",");
        try {
            env.execute("TrajectoryKNN");
        } catch (Exception e){
            e.printStackTrace();
        }
        stopwatch.stop();
        System.out.println(new Long(stopwatch.getTime()).doubleValue() / 1000 + " s");
    }
}
