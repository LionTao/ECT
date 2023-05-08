package cn.edu.suda.ada.strajdb.pushdown;

import lombok.Builder;
import lombok.Data;
import lombok.var;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class SpatialQuery {
    @Data
    @Builder
    private static class Result {
        int QueryNo;
        double Traffic;
        long time;
        String Name;

    }


    private static final Configuration hadoopConfig = new Configuration();

    static {
        hadoopConfig.set("fs.alluxio.impl", "alluxio.hadoop.FileSystem");
        hadoopConfig.set("fs.AbstractFileSystem.alluxio.impl", "alluxio.hadoop.AlluxioFileSystem");
        hadoopConfig.set("fs.s3a.access.key", "minioadmin");
        hadoopConfig.set("fs.s3a.secret.key", "minioadmin");
        hadoopConfig.set("fs.s3a.endpoint", "http://192.168.134.126:9000");
        hadoopConfig.set("fs.defaultFS", "192.168.134.126:8020");
        hadoopConfig.setBoolean("dfs.client.use.datanode.hostname", true);
        hadoopConfig.set("dfs.replication", "3");
        hadoopConfig.setBoolean("fs.hdfs.impl.disable.cache", true);
        hadoopConfig.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
    }

    public static void main(String[] args) throws IOException {
        List<Result> csvResult = new LinkedList<>();
        int NUM = 6;
        List<float[]> rects = new ArrayList<>();

        // read traj from id 1 to 10
        try (FileReader f = new FileReader("/var/home/liontao/work/stdtw/trans-raw.txt")) {
            BufferedReader in = new BufferedReader(f);
            String str;
            while ((str = in.readLine()) != null) {
                String[] line = str.trim().split(",");
                if (line.length == 5) {
                    var temp = new float[]{Float.parseFloat(line[0]), Float.parseFloat(line[1]), Float.parseFloat(line[2]), Float.parseFloat(line[3])};
                    rects.add(temp);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // HDFS
        var p = new Path("/hdfsexp/all.parquet");
        long start = System.currentTimeMillis();
        var startRx = RxMB();
//        for (int i = 1; i < NUM; i++) {
//
//            var predicate = FilterApi.and(
//                    FilterApi.and(
//                            FilterApi.gtEq(FilterApi.floatColumn("lon"), rects.get(i - 1)[0]),
//                            FilterApi.ltEq(FilterApi.floatColumn("lon"), rects.get(i - 1)[2])
//                    ),
//                    FilterApi.and(
//                            FilterApi.gtEq(FilterApi.floatColumn("lat"), rects.get(i - 1)[1]),
//                            FilterApi.ltEq(FilterApi.floatColumn("lat"), rects.get(i - 1)[3])
//                    )
//            );
//            try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(HadoopInputFile.fromPath(p, hadoopConfig))
//                    .withFilter(FilterCompat.get(predicate))
//                    .useBloomFilter()
//                    .useRecordFilter()
//                    .build()) {
//                GenericRecord nextRecord = reader.read();
//                while (nextRecord != null) {
//                    System.out.println(nextRecord);
//                    nextRecord = reader.read();
//                }
//            }
//            var endRx = RxMB() - startRx;
//            var push = System.currentTimeMillis() - start;
//            csvResult.add(Result.builder()
//                    .QueryNo(i)
//                    .Name("hdfs-id")
//                    .time(push)
//                    .Traffic(endRx)
//                    .build());
//        }
//        writeCSV(csvResult, "pushdown-spatial-hdfs.csv");
        csvResult = new LinkedList<>();
        // plain-oss
        p = new Path("s3a://warehouse/all.parquet");
        start = System.currentTimeMillis();
        startRx = RxMB();
//        for (int i = 1; i < NUM; i++) {
//            var predicate = FilterApi.and(
//                    FilterApi.and(
//                            FilterApi.gtEq(FilterApi.floatColumn("lon"), rects.get(i - 1)[0]),
//                            FilterApi.ltEq(FilterApi.floatColumn("lon"), rects.get(i - 1)[2])
//                    ),
//                    FilterApi.and(
//                            FilterApi.gtEq(FilterApi.floatColumn("lat"), rects.get(i - 1)[1]),
//                            FilterApi.ltEq(FilterApi.floatColumn("lat"), rects.get(i - 1)[3])
//                    )
//            );
//            try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(HadoopInputFile.fromPath(p, hadoopConfig))
//                    .withFilter(FilterCompat.get(predicate))
//                    .useBloomFilter()
//                    .useRecordFilter()
//                    .build()) {
//                GenericRecord nextRecord = reader.read();
//                while (nextRecord != null) {
//                    System.out.println(nextRecord);
//                    nextRecord = reader.read();
//                }
//            }
//            var endRx = RxMB() - startRx;
//            var push = System.currentTimeMillis() - start;
//            csvResult.add(Result.builder()
//                    .QueryNo(i)
//                    .Name("oss-plain")
//                    .time(push)
//                    .Traffic(endRx)
//                    .build());
//        }
//        writeCSV(csvResult, "pushdown-spatial-oss-plain.csv");
        csvResult = new LinkedList<>();
        // our
        CloseableHttpClient httpclient = HttpClients.createDefault();
        ResponseHandler<String> responseHandler = response -> {
            int status = response.getStatusLine().getStatusCode();
            if (status >= 200 && status < 300) {
                HttpEntity entity = response.getEntity();
                return entity != null ? EntityUtils.toString(entity) : null;
            } else {
                throw new ClientProtocolException("Unexpected response status: " + status);
            }
        };
        start = System.currentTimeMillis();
        startRx = RxMB();
        for (int i = 1; i < NUM; i++) {
            HttpGet httpget = new HttpGet(String.format("http://131-195:60000/spatial?minX=%f&minY=%f&maxX=%f&maxY=%f", rects.get(i - 1)[0], rects.get(i - 1)[1], rects.get(i - 1)[2], rects.get(i - 1)[3]));
            System.out.println(httpclient.execute(httpget, responseHandler));
            var endRx = RxMB() - startRx;
            var push = System.currentTimeMillis() - start;
            csvResult.add(Result.builder()
                    .QueryNo(i)
                    .Name("pushdown")
                    .time(push)
                    .Traffic(endRx)
                    .build());
        }
        writeCSV(csvResult, "pushdown-spatial-pushdown.csv");


    }

    public static double RxMB() {
        String[] cmd = {"/bin/sh", "-c", "ip -s link show wlp3s0"};
        try {
            Process p = Runtime.getRuntime().exec(cmd);
            BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line;
            double rxBytes = 0;
            while ((line = reader.readLine()) != null) {
                if (line.contains("RX:")) {
                    line = reader.readLine();
                    System.out.println(line);
                    String[] tokens = line.trim().split("\\s+");
                    rxBytes = Double.parseDouble(tokens[0]) / 1024 / 1024;
                    break;
                }
            }
            reader.close();
            return rxBytes;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    private static void writeCSV(List<Result> csvResult, String fname) throws IOException {
        StringWriter sw = new StringWriter();
        String[] HEADERS = {"name", "number", "time", "traffic"};
        CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                .setHeader(HEADERS)
                .build();
        try (final CSVPrinter printer = new CSVPrinter(sw, csvFormat)) {
            csvResult.forEach((c) -> {
                try {
                    printer.printRecord(c.getName(), c.getQueryNo(), c.getTime(), c.getTraffic());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
        BufferedWriter writer = new BufferedWriter(new FileWriter(fname));
        writer.write(sw.toString());

        writer.close();
    }
}
