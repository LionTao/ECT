package cn.edu.suda.ada.strajdb.query;

import com.github.davidmoten.rtree2.geometry.Geometries;
import lombok.Builder;
import lombok.Data;
import lombok.var;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class HiveBench {
    @Data
    @Builder
    private static class Result {
        int QueryNo;
        int TrajSize;
        long time;
        String Name;
    }

    private static final String driverName = "org.apache.hive.jdbc.HiveConnection";

    public static void main(String[] args) throws IOException, SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        TreeMap<String, List<String>> inverted = new TreeMap<String, List<String>>();
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
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        System.out.println(inverted.size());
        // clean exp dir

        StringWriter sw = new StringWriter();
        String[] HEADERS = {"name", "number", "time", "candidates"};
        CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                .setHeader(HEADERS)
                .build();


        int NUM_THREADS = 10;
        var candidates = (new ArrayList<>(inverted.keySet())).stream().sorted().collect(Collectors.toList());
        long start = System.currentTimeMillis();
        List<Result> csvResult = new LinkedList<>();
        for (int i = 0; i < 7901; i = i + 10) {
            System.out.println(i + "-" + (i + 10));
            ExecutorService pool = Executors.newFixedThreadPool(NUM_THREADS);
            for (var name :
                    candidates.subList(i, i + 10)) {
                pool.execute(() -> {
                    try {
                        sendMessage(name, inverted);
                    } catch (IOException | SQLException e) {
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
            csvResult.add(Result.builder()
                    .Name("hive")
                    .QueryNo(i + 1000)
                    .time(System.currentTimeMillis() - start)
                    .TrajSize(inverted.size())
                    .build());
            System.out.println(System.currentTimeMillis() - start);

        }
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
        BufferedWriter writer = new BufferedWriter(new FileWriter("insert-hive.csv"));
        writer.write(sw.toString());
        writer.close();
    }

    private static void sendMessage(String name, TreeMap<String, List<String>> inverted) throws IOException, SQLException {
        Connection con = DriverManager.getConnection(
                "jdbc:hive2://131-195:10000", "hive", "");
        Statement stmt = con.createStatement();
        String sql = "DROP TABLE IF EXISTS t_" + name;
        stmt.execute(sql);
        sql = String.format("CREATE TABLE IF NOT EXISTS t_%s (id INT,traj String,ts BIGINT,lon DOUBLE,lat DOUBLE)", name);

        stmt.execute(sql);
        int c = 0;
        for (var part :
                inverted.get(name)) {

            try (FileReader f = new FileReader("/var/home/liontao/Documents/data/filtered/" + part)) {
                BufferedReader in = new BufferedReader(f);
                String str;
                StringBuilder sb = new StringBuilder();
                sb.append(String.format("INSERT INTO t_%s VALUES ",name));

                while ((str = in.readLine()) != null) {
                    String[] line = str.trim().split(",");
                    if (line.length == 4) {
                        sb.append(String.format(" (%s,%s,%s,%s,%s),",c,line[0],System.currentTimeMillis(),line[2],line[3]));
                        c += 1;
                    }
                }
                //write
                sb.delete(sb.length()-1,sb.length());
                var s = sb.toString();
                stmt.execute(s);
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
    }
}
