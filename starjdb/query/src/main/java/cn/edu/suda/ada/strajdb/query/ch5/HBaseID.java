package cn.edu.suda.ada.strajdb.query.ch5;

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
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

import java.io.*;
import java.util.*;

public class HBaseID {
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
        System.out.println(inverted.size());
        int QUERIES = 5000;
        List<String> keysAsArray = new ArrayList<>(bucket.keySet());
        Random r = new Random();
        var candidatesSet = new LinkedList<String>();
        for (int i = 0; i < QUERIES; i++) {
            candidatesSet.add(keysAsArray.get(r.nextInt(keysAsArray.size())));
        }
        int i = 0;
        long start = System.currentTimeMillis();
        for (var candidates : candidatesSet) {
            i++;
            var curr = System.currentTimeMillis();
            var parent = candidates.split("_")[0];

            //read
            Table t = connection.getTable(TableName.valueOf(parent));
            var scan = new Scan();
            var scanner = t.getScanner(scan);
            var sb = new StringBuilder();
            for (Iterator<org.apache.hadoop.hbase.client.Result> it = scanner.iterator(); it.hasNext(); ) {
                org.apache.hadoop.hbase.client.Result temp = it.next();
                if (temp!=null){
                    sb.append(temp);
                }
            }
            var length = sb.toString().length();


            if (i%1000==0){
                System.out.printf("hbase %d\n", System.currentTimeMillis() - start);
                csvResult.add(Result.builder()
                        .QueryNo(i)
                        .Name("hbase")
                        .time(System.currentTimeMillis() - start)
                        .TrajSize(1)
                        .build());
            }
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
        BufferedWriter writer = new BufferedWriter(new FileWriter("id-hbase.csv"));
        writer.write(sw.toString());
        writer.close();
    }
}
