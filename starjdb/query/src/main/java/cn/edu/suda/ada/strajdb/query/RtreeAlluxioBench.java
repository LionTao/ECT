package cn.edu.suda.ada.strajdb.query;

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InternalException;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.FreePOptions;
import com.github.davidmoten.rtree2.RTree;
import com.github.davidmoten.rtree2.geometry.Geometries;
import com.github.davidmoten.rtree2.geometry.Rectangle;
import io.minio.GetObjectArgs;
import io.minio.MinioClient;
import io.minio.errors.*;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.var;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class RtreeAlluxioBench {
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
        if (fs.exists(dpath)) {
            fs.delete(dpath, DeletePOptions.newBuilder().setRecursive(true).build());
        }
        fs.createDirectory(dpath);
        AlluxioURI dpath2 = new AlluxioURI("/filtered");
        fs.free(dpath2, FreePOptions.newBuilder().setRecursive(true).build());
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
                    var env = Geometries.rectangle(Double.parseDouble(line[0]), Double.parseDouble(line[1]), Double.parseDouble(line[2]), Double.parseDouble(line[3]));
                    tree = tree.add(line[4], env);
                    bucket.put(line[4], env);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        System.out.println(inverted.size());
        int QUERIES = 40;
        List<String> keysAsArray = bucket.keySet().stream().sorted().collect(Collectors.toList());
        var candidatesSet = new LinkedList<Iterable<com.github.davidmoten.rtree2.Entry<String, Rectangle>>>();
        for (int i = 0; i < QUERIES; i++) {
            candidatesSet.add(tree.search(bucket.get(keysAsArray.get(i))));
        }
        int i = 0;
        long start = System.currentTimeMillis();
        for (var candidates : candidatesSet) {
            i++;
            var curr = System.currentTimeMillis();
            Set<String> garden = new HashSet<>();
            for (var f :
                    candidates) {
                // gather trajectory id
                garden.add(f.value().split("_")[0]);
            }
            // merge files
            ExecutorService pool = Executors.newFixedThreadPool(10);
            for (String fname :
                    garden) {
                pool.execute(() -> {
                    try {
                        readOneTraj(fname, inverted);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            }
            pool.shutdown();
            try {
                var t = pool.awaitTermination(9999, TimeUnit.DAYS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            System.out.printf("alluxio %d\n", System.currentTimeMillis() - start);
            csvResult.add(Result.builder()
                    .QueryNo(i)
                    .Name("alluxio")
                    .time(System.currentTimeMillis() - start)
                    .TrajSize(garden.size())
                    .build());
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
        BufferedWriter writer = new BufferedWriter(new FileWriter("topk-alluxio.csv"));
        writer.write(sw.toString());

        writer.close();
        i = 0;
        csvResult = new LinkedList<>();
        start = System.currentTimeMillis();
        for (var candidates : candidatesSet) {
            i++;
            var curr = System.currentTimeMillis();
            Set<String> garden = new HashSet<>();
            for (var f :
                    candidates) {
                // gather trajectory id
                garden.add(f.value().split("_")[0]);
            }
            ExecutorService pool = Executors.newFixedThreadPool(10);
            for (String fname :
                    garden) {
                pool.execute(() -> {
                    try {
                        readOneTrajMinio(fname, inverted);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            }
            pool.shutdown();
            try {
                var t = pool.awaitTermination(9999, TimeUnit.DAYS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            System.out.printf("minio %d\n", System.currentTimeMillis() - start);
            csvResult.add(Result.builder()
                    .QueryNo(i)
                    .Name("minio")
                    .time(System.currentTimeMillis() - start)
                    .TrajSize(garden.size())
                    .build());
        }
        System.out.printf("minio %d\n", System.currentTimeMillis() - start);

        sw = new StringWriter();

        try (final CSVPrinter printer = new CSVPrinter(sw, csvFormat)) {
            csvResult.forEach((c) -> {
                try {
                    printer.printRecord(c.Name, c.QueryNo, c.time, c.TrajSize);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
        writer = new BufferedWriter(new FileWriter("topk-minio.csv"));
        writer.write(sw.toString());

        writer.close();
    }

    public static Set<Path> listFilesUsingFilesList(String dir) {
        try (Stream<Path> stream = java.nio.file.Files.list(Paths.get(dir))) {
            return stream
                    .filter(file -> !Files.isDirectory(file))
                    .map(Path::toAbsolutePath)
                    .collect(Collectors.toSet());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void readOneTraj(String fname, HashMap<String, List<String>> inverted) throws IOException, AlluxioException {
        AlluxioURI path = new AlluxioURI("/garden/" + fname);
        if (fs.exists(path)) {
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
            var t = resultStringBuilder.toString().length();
        } else {
            StringBuilder resultStringBuilder = new StringBuilder();
            for (String sfname :
                    inverted.get(fname)) {
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
            // write big file
            FileOutStream out = fs.createFile(path);
            out.write(resultStringBuilder.toString().getBytes(StandardCharsets.UTF_8));
            out.close();
        }
    }

    public static void readOneTrajMinio(String fname, HashMap<String, List<String>> inverted) throws IOException, AlluxioException {
        for (String sfname :
                inverted.get(fname)) {
            try (InputStream in = minioClient.getObject(
                    GetObjectArgs.builder()
                            .bucket("warehouse")
                            .object("filtered/filtered/" + sfname)
                            .build())) {
                // Read data from stream
                StringBuilder resultStringBuilder = new StringBuilder();
                try (BufferedReader br
                             = new BufferedReader(new InputStreamReader(in))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        resultStringBuilder.append(line).append("\n");
                    }
                }
                var t = resultStringBuilder.toString().length();
            } catch (InternalException | ErrorResponseException | InsufficientDataException |
                     io.minio.errors.InternalException | InvalidKeyException | InvalidResponseException |
                     NoSuchAlgorithmException | ServerException | XmlParserException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
