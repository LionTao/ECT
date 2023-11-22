package ect.baseline.Operator;

import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.LineString;
import GeoFlink.spatialObjects.Point;
import lombok.var;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Coordinate;

import java.util.LinkedList;

public class MergePointsInWindowToTrajectory implements WindowFunction<Point, LineString, String, TimeWindow> {
    UniformGrid uGrid;

    public MergePointsInWindowToTrajectory(UniformGrid uGrid) {
        this.uGrid = uGrid;
    }

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
        if(points.size()>1){
            var line = new LineString(objID, points, time, uGrid);
            out.collect(line);
        }
    }
}
