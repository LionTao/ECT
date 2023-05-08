package GeoFlink;

import GeoFlink.apps.CheckIn;
import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.LineString;
import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialOperators.QueryConfiguration;
import GeoFlink.spatialOperators.QueryType;
import GeoFlink.spatialOperators.knn.LineStringLineStringKNNQuery;
import GeoFlink.spatialStreams.Deserialization;
import GeoFlink.utils.Params;
import lombok.var;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Coordinate;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.LinkedList;
import java.util.PriorityQueue;

public class MyJob {
    public static void main(String[] args) throws Exception {
        StopWatch stopwatch = new StopWatch();
        stopwatch.start();
        Configuration config = new Configuration();
        config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
        config.setString(RestOptions.BIND_PORT, "8082");
        //Creating uniform grid index by defining its boundaries
        double minX = 115.50, maxX = 117.60, minY = 39.60, maxY = 41.10;
        int gridSize = 100;
        UniformGrid uGrid = new UniformGrid(gridSize, minX, maxX, minY, maxY);

        //Declaring query variables
        double radius = 0.05; // 经度每隔0.00001度，距离相差约1米 0.05=5000m
        int k = 100;

        Params params = new Params();
        System.out.println(params);

//        int allowedLateness = params.queryOutOfOrderTuples;
        int allowedLateness = 999999999;
        /* Windows */
        int windowSize = 100;
        int windowSlideStep = 100;

        QueryConfiguration windowConf = new QueryConfiguration(QueryType.WindowBased);
        windowConf.setApproximateQuery(false);
        windowConf.setAllowedLateness(allowedLateness);
        windowConf.setWindowSize(windowSize);
        windowConf.setSlideStep(windowSlideStep);

        QueryConfiguration stringWindowConf = new QueryConfiguration(QueryType.WindowBased);
        windowConf.setApproximateQuery(false);
        windowConf.setAllowedLateness(allowedLateness);
        windowConf.setWindowSize(10);
        windowConf.setSlideStep(10);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

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

        // 主要思路：
        // 1. 数据以点的形式进入系统
        //    每个轨迹内部点时间间隔为1s，方便后续调整窗口大小
        // 2. 对点进行分区和时间窗口聚合
        // 3. 依照查询轨迹先找到所有相关的轨迹分区，然后拉出所有的轨迹id，组装linestring stream
        // 4. 拉出对应的所有点组成轨迹进行计算
        // 注： 核心是利用这人的uGrid

        var fileStream = env.readTextFile("/home/liontao/work/stream-distance/notebooks/preprocess/tdrive-geoflink-long.txt");
        DataStream<Point> pointStream = Deserialization.MyTrajectoryStream(fileStream, uGrid);

        // pointStream->windowed point stream

        var lingStringStream = pointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
            @Override
            public long extractTimestamp(Point p) {
                return p.timeStampMillisec;
            }
        }).keyBy(new KeySelector<Point, String>() {
            @Override
            public String getKey(Point point) throws Exception {
                return point.objID;
            }
        }).window(
                SlidingEventTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSlideStep))
        ).trigger(new CheckIn.onEventTimeTrigger()).apply(new WindowFunction<Point, LineString, String, TimeWindow>() {
            @Override
            public void apply(String objID, TimeWindow window, Iterable<Point> input, Collector<LineString> out) throws Exception {
                var points = new LinkedList<Coordinate>();
//                var gridSet = new HashSet<String>();
                long time = Long.MIN_VALUE;
                for (Point p :
                        input) {
                    points.add(p.point.getCoordinate());
//                    gridSet.add(p.gridID);
                    time = Math.max(p.timeStampMillisec, time);
                }
                var line = new LineString(objID, points, time, uGrid);
                out.collect(line);
            }
        });
        // 目标是凑这个输入形式，这里的轨迹需要在gridset里包含多个grid，ts以最新的点为准
//        lingStringStream.print();
        new LineStringLineStringKNNQuery(windowConf, uGrid).run(lingStringStream, queryLineString, radius, k).map(new MapFunction<Tuple3<Long, Long, PriorityQueue<Tuple2<LineString, Double>>>, Integer>() {
            @Override
            public Integer map(Tuple3<Long, Long, PriorityQueue<Tuple2<LineString, Double>>> o) throws Exception {
                return o.f2.size();
            }
        }).print();
        env.execute("Geo Flink");
        stopwatch.stop();
        System.out.println(new Long(stopwatch.getTime()).doubleValue() / 1000 + " s");
    }
}
