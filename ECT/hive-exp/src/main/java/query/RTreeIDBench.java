package query;

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import com.github.davidmoten.rtree2.RTree;
import com.github.davidmoten.rtree2.geometry.Geometries;
import com.github.davidmoten.rtree2.geometry.Rectangle;
import io.minio.MinioClient;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.*;
import java.util.*;

public class RTreeIDBench {
    @Data
    @Builder
    private static class Result {
        int QueryNo;
        int TrajSize;
        long time;
        String Name;
    }

    private static final FileSystem fs = FileSystem.Factory.get();
    static MinioClient minioClient =
            MinioClient.builder()
                    .endpoint("http://192.168.134.126:9000")
                    .credentials("minioadmin", "minioadmin")
                    .build();

    @SneakyThrows
    public static void main(String[] args) {
        List<Result> csvResult = new LinkedList<>();
        AlluxioURI dpath = new AlluxioURI("/garden");
        if (!fs.exists(dpath)) {
            fs.createDirectory(dpath);
        }
        RTree<String, Rectangle> tree = RTree.star().maxChildren(6).create();
        HashMap<String, Rectangle> bucket = new HashMap<>();
        HashMap<String, List<String>> inverted = new HashMap<>();
        try (FileReader f = new FileReader("/var/home/liontao/work/stdtw/trans.txt")) {
            BufferedReader in = new BufferedReader(f);
            String str;
            while ((str = in.readLine()) != null) {
                String[] line = str.trim().split(",");
                if (line.length == 5) {
                    String parentFname = line[4].split("_")[0];
                    List<String> l = inverted.getOrDefault(parentFname, new LinkedList<>());
                    l.add(line[4]);
                    inverted.put(parentFname, l);
                    Rectangle env = Geometries.rectangle(Double.parseDouble(line[0]), Double.parseDouble(line[1]), Double.parseDouble(line[2]), Double.parseDouble(line[3]));
                    tree = tree.add(line[4], env);
                    bucket.put(line[4], env);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        System.out.println(inverted.size());
        int QUERIES = 5000;
        List<String> keysAsArray = new ArrayList<>(bucket.keySet());
        Random r = new Random();
        List<String> candidatesSet = new LinkedList<String>();
        for (int i = 0; i < QUERIES; i++) {
            candidatesSet.add(keysAsArray.get(r.nextInt(keysAsArray.size())));
        }
        int i = 0;
        long start = System.currentTimeMillis();
        for (String candidates : candidatesSet) {
            i++;
            long curr = System.currentTimeMillis();
            String parent = candidates.split("_")[0];
            // merge files
            AlluxioURI path = new AlluxioURI("/garden/" + candidates);
            if (false) {
//            if (fs.exists(path)) {
                // file existed
                FileInStream in = fs.openFile(path);
                StringBuilder resultStringBuilder = new StringBuilder();
                try (BufferedReader br
                             = new BufferedReader(new InputStreamReader(in))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        resultStringBuilder.append(line).append("\n");
                    }
                }
                int t = resultStringBuilder.toString().length();
            } else {
                StringBuilder resultStringBuilder = new StringBuilder();
                for (String sfname :
                        inverted.get(parent)) {
                    AlluxioURI spath = new AlluxioURI("/filtered/filtered/" + sfname);
                    FileInStream in = fs.openFile(spath);
                    try (BufferedReader br
                                 = new BufferedReader(new InputStreamReader(in))) {
                        String line;
                        while ((line = br.readLine()) != null) {
                            resultStringBuilder.append(line).append("\n");
                        }
                    }
                }
                String temp = resultStringBuilder.toString();
                // write big file
//                FileOutStream out = fs.createFile(path);
//                out.write(resultStringBuilder.toString().getBytes(StandardCharsets.UTF_8));
//                out.close();
            }

            if (i % 1000 == 0) {
                csvResult.add(Result.builder()
                        .QueryNo(i)
                        .Name("alluxio")
                        .time(System.currentTimeMillis() - start)
                        .TrajSize(1)
                        .build());
            }
        }
        System.out.printf("alluxio %d\n", System.currentTimeMillis() - start);
        StringWriter sw = new StringWriter();
        String[] HEADERS = {"name", "number", "time", "candidates"};
        CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                .setHeader(HEADERS)
                .build();

        try (final CSVPrinter printer = new CSVPrinter(sw, csvFormat)) {
            csvResult.forEach((c) -> {
                try {
                    printer.printRecord(c.Name, c.QueryNo, c.time, c.TrajSize);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
        BufferedWriter writer = new BufferedWriter(new FileWriter("id-alluxio-hot-nomr.csv"));
        writer.write(sw.toString());
        writer.close();

        // clear results
//        csvResult.clear();
//        i = 0;
//        start = System.currentTimeMillis();
//        for (var candidates : candidatesSet) {
//            i++;
//            var curr = System.currentTimeMillis();
//            var parent = candidates.split("_")[0];
//
//            for (String sfname :
//                    inverted.get(parent)) {
//                try (InputStream in = minioClient.getObject(
//                        GetObjectArgs.builder()
//                                .bucket("warehouse")
//                                .object("filtered/filtered/" + sfname)
//                                .build())) {
//                    // Read data from stream
//                    StringBuilder resultStringBuilder = new StringBuilder();
//                    try (BufferedReader br
//                                 = new BufferedReader(new InputStreamReader(in))) {
//                        String line;
//                        while ((line = br.readLine()) != null) {
//                            resultStringBuilder.append(line).append("\n");
//                        }
//                    }
//                    var t = resultStringBuilder.toString().length();
//                } catch (InternalException e) {
//                    throw new RuntimeException(e);
//                }
//            }
//
//            if (i%1000==0){
//                csvResult.add(Result.builder()
//                        .QueryNo(i)
//                        .Name("minio")
//                        .time(System.currentTimeMillis() - start)
//                        .TrajSize(1)
//                        .build());
//            }
//        }
//        System.out.printf("minio %d\n", System.currentTimeMillis() - start);
//
//        sw = new StringWriter();
//
//        try (final CSVPrinter printer = new CSVPrinter(sw, csvFormat)) {
//            csvResult.forEach((c) -> {
//                try {
//                    printer.printRecord(c.Name, c.QueryNo, c.time, c.TrajSize);
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            });
//        }
//        writer = new BufferedWriter(new FileWriter("id-minio.csv"));
//        writer.write(sw.toString());
//        writer.close();
    }
}
