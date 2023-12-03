package query;

import lombok.Builder;
import lombok.Data;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class HBaseBench {
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
        TreeMap<String, List<String>> inverted = new TreeMap<String, List<String>>();
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
//                    var env = Geometries.rectangle(Double.parseDouble(line[0]), Double.parseDouble(line[1]), Double.parseDouble(line[2]), Double.parseDouble(line[3]));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        System.out.println(inverted.size());

        StringWriter sw = new StringWriter();
        String[] HEADERS = {"name", "number", "time", "candidates"};
        CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                .setHeader(HEADERS)
                .build();


        int NUM_THREADS = 1000;
        List<String> candidates = (new ArrayList<>(inverted.keySet())).stream().sorted().collect(Collectors.toList());
        long start = System.currentTimeMillis();
        List<Result> csvResult = new LinkedList<>();
        for (int i = 0; i < 7001; i = i + 1000) {
            System.out.println(i + "-" + (i + 1000));
            ExecutorService pool = Executors.newFixedThreadPool(NUM_THREADS);
            for (String name :
                    candidates.subList(i, i + 1000)) {
                pool.execute(() -> {
                    try {
                        sendMessage(name, inverted);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
            pool.shutdown();
            try {
                pool.awaitTermination(9999, TimeUnit.DAYS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            csvResult.add(Result.builder()
                    .Name("hbase")
                    .QueryNo(i + 1000)
                    .time(System.currentTimeMillis() - start)
                    .TrajSize(inverted.size())
                    .build());
            System.out.println(System.currentTimeMillis() - start);

        }
        ExecutorService pool = Executors.newFixedThreadPool(NUM_THREADS);
        for (String name :
                candidates.subList(8000, candidates.size())) {
            pool.execute(() -> {
                try {
                    sendMessage(name, inverted);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        pool.shutdown();
        try {
            boolean t = pool.awaitTermination(9999, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        csvResult.add(Result.builder()
                .Name("hbase")
                .QueryNo(candidates.size())
                .time(System.currentTimeMillis() - start)
                .TrajSize(inverted.size())
                .build());
        System.out.println(System.currentTimeMillis() - start);
        System.out.println("\n\n\n\nDone.");
        try (final CSVPrinter printer = new CSVPrinter(sw, csvFormat)) {
            csvResult.forEach((c) -> {
                try {
                    printer.printRecord(c.Name, c.QueryNo, c.time, c.TrajSize);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
        BufferedWriter writer = new BufferedWriter(new FileWriter("insert-hbase.csv"));
        writer.write(sw.toString());
        writer.close();
    }

    private static void sendMessage(String name, TreeMap<String, List<String>> inverted) throws IOException {
        org.apache.hadoop.hbase.client.Admin admin = connection.getAdmin();
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(name));
        table.addFamily(new HColumnDescriptor("DEFAULT").setCompressionType(Compression.Algorithm.NONE));
        if (admin.tableExists(TableName.valueOf(name))) {
            System.out.println("exists!");
            admin.disableTable(TableName.valueOf(name));
            admin.deleteTable(TableName.valueOf(name));
        }
        admin.createTable(table);
        Table t = connection.getTable(TableName.valueOf(name));
        int c = 0;
        for (String part :
                inverted.get(name)) {
            try (FileReader f = new FileReader("/var/home/liontao/Documents/data/filtered/" + part)) {
                BufferedReader in = new BufferedReader(f);
                String str;
//                StringBuilder sb = new StringBuilder();
                while ((str = in.readLine()) != null) {
                    String[] line = str.trim().split(",");
                    Put put1 = new Put(Bytes.toBytes(c));
                    put1.addColumn(Bytes.toBytes("DEFAULT"), Bytes.toBytes("id"),
                            Bytes.toBytes(line[0]));
                    put1.addColumn(Bytes.toBytes("DEFAULT"), Bytes.toBytes("ts"),
                            Bytes.toBytes(System.currentTimeMillis()));
                    put1.addColumn(Bytes.toBytes("DEFAULT"), Bytes.toBytes("lon"),
                            Bytes.toBytes(line[2]));
                    put1.addColumn(Bytes.toBytes("DEFAULT"), Bytes.toBytes("lat"),
                            Bytes.toBytes(line[3]));
                    c += 1;
                    t.put(put1);
                }
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
        t.close();
        admin.close();
    }
}
