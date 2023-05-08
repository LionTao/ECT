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
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class HBaseTopk {
    @Data
    @Builder
    private static class Result {
        int QueryNo;
        int TrajSize;
        long time;
        String Name;
    }

    private static Connection connection;

    static {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
// 如果是集群 则主机名用逗号分隔
        configuration.set("hbase.zookeeper.quorum", "192.168.134.126");
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        var admin = connection.getAdmin();
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
                    var parentFname = line[4].split("_")[0];
                    var l = inverted.getOrDefault(parentFname, new LinkedList<>());
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
            System.out.printf("hbase %d\n", System.currentTimeMillis() - start);
            csvResult.add(Result.builder()
                    .QueryNo(i)
                    .Name("hbase")
                    .time(System.currentTimeMillis() - start)
                    .TrajSize(garden.size())
                    .build());
        }
        System.out.printf("hbase %d\n", System.currentTimeMillis() - start);
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
        BufferedWriter writer = new BufferedWriter(new FileWriter("topk-hbase.csv"));
        writer.write(sw.toString());

        writer.close();
    }

    public static void readOneTraj(String fname, HashMap<String, List<String>> inverted) throws IOException, AlluxioException {
        Table t = connection.getTable(TableName.valueOf(fname));
        var scan = new Scan();
        var scanner = t.getScanner(scan);
        var sb = new StringBuilder();
        for (org.apache.hadoop.hbase.client.Result temp : scanner) {
            if (temp != null) {
                sb.append(temp);
            }
        }
        var length = sb.toString().length();
    }
}
