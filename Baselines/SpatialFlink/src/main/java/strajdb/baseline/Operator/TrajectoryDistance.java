package strajdb.baseline.Operator;

import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.LineString;
import lombok.var;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TrajectoryDistance {
    private static final Logger LOG = LoggerFactory.getLogger(TrajectoryDistance.class);

    public static double rad = Math.PI / 180.0;
    public static double R = 6378137.0;

    public static double Frechet(final LineString x, final LineString y){
        throw new NotImplementedException("No Frechet available!");
    }

    public static double DTW(final LineString x, final LineString y, UniformGrid uGrid) {
        var t0 = x.lineString;
        var t1 = y.lineString;
        int n0 = t0.getNumPoints();
        int n1 = t1.getNumPoints();

        double[][] C = new double[n0 + 1][n1 + 1];
        for (int i = 1; i < n0 + 1; i++) {
            C[i][0] = Double.MAX_VALUE;
        }
        for (int j = 1; j < n1 + 1; j++) {
            C[0][j] = Double.MAX_VALUE;
        }
        for (int i = 1; i < n0 + 1; i++) {
            for (int j = 1; j < n1 + 1; j++) {
                C[i][j] = GreatCircleDistance(t0.getCoordinateN(i-1).x,t0.getCoordinateN(i-1).y,
                        t1.getCoordinateN(j-1).x,t1.getCoordinateN(j-1).y) +
                        Math.min(C[i][j-1],Math.min(C[i-1][j-1],C[i-1][j]));
            }
        }
        double res = C[n0][n1]/(uGrid.getCellLength()*111000000000L); // adapt for inaccurate rad distance
//        System.out.println("DTW:"+res);
        return res;
    }
    // 116.39,39.5
    // 116.40,39.6
    public static double GreatCircleDistance(double lon1, double lat1, double lon2, double lat2) {
        double dlat = rad * (lat2 - lat1);
        double dlon = rad * (lon2 - lon1);
        double a = (Math.sin(dlat / 2.0) * Math.sin(dlat / 2.0) +
                Math.cos(rad * lat1) * Math.cos(rad * lat2) *
                        Math.sin(dlon / 2.0) * Math.sin(dlon / 2.0));
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return R * c;
    }
}
