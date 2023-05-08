package cn.edu.suda.ada.strajdb.query.ch5;

import alluxio.exception.AlluxioException;
import com.github.davidmoten.rtree2.RTree;
import com.github.davidmoten.rtree2.geometry.Geometries;
import com.github.davidmoten.rtree2.geometry.Rectangle;
import lombok.Builder;
import lombok.Data;
import lombok.var;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class HDFSTopK {

    @Data
    @Builder
    private static class Result {
        int QueryNo;
        int TrajSize;
        long time;
        String Name;
    }

    private static final Configuration hadoopConfig = new Configuration();
    private static final org.apache.hadoop.fs.FileSystem fileSystem;

    static {
        hadoopConfig.set("fs.defaultFS", "131-195:8020");
        hadoopConfig.setBoolean("dfs.client.use.datanode.hostname", true);
        hadoopConfig.set("dfs.replication", "3");
        hadoopConfig.setBoolean("fs.hdfs.impl.disable.cache", true);
        hadoopConfig.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        try {
            fileSystem = FileSystem.get(hadoopConfig);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws IOException {
        List<Result> csvResult = new LinkedList<>();
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
            System.out.printf("hdfs %d\n", System.currentTimeMillis() - start);
            csvResult.add(Result.builder()
                    .QueryNo(i)
                    .Name("hdfs")
                    .time(System.currentTimeMillis() - start)
                    .TrajSize(garden.size())
                    .build());
        }
        System.out.printf("hdfs %d\n", System.currentTimeMillis() - start);
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
        BufferedWriter writer = new BufferedWriter(new FileWriter("topk-hdfs.csv"));
        writer.write(sw.toString());

        writer.close();
    }

    public static void readOneTraj(String fname, HashMap<String, List<String>> inverted) throws IOException, AlluxioException {
        String trajPath = "/hdfsexp/tdrive/" + fname + ".txt";
        Path hdfsPath = new Path(trajPath);
        var in = fileSystem.open(hdfsPath);
        StringBuilder resultStringBuilder = new StringBuilder();
        try (BufferedReader br
                     = new BufferedReader(new InputStreamReader(in))) {
            String line;
            while ((line = br.readLine()) != null) {
                resultStringBuilder.append(line).append("\n");
            }
        }
        var t = resultStringBuilder.toString().length();
    }
}
