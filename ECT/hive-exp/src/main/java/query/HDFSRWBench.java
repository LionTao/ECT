package query;

import com.github.davidmoten.rtree2.geometry.Geometries;
import lombok.Builder;
import lombok.Data;
import lombok.var;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.util.StopWatch;

import java.io.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class HDFSRWBench {
    @Data
    @Builder
    private static class Result{
        int QueryNo;
        int TrajSize;
        long time;
        String Name;
    }
    private static final Configuration hadoopConfig = new Configuration();

    static {
        hadoopConfig.set("fs.defaultFS", "131-195:8020");
        hadoopConfig.setBoolean("dfs.client.use.datanode.hostname", true);
        hadoopConfig.set("dfs.replication", "3");
        hadoopConfig.setBoolean("fs.hdfs.impl.disable.cache", true);
        hadoopConfig.set("dfs.client.block.write.replace-datanode-on-failure.policy","NEVER");
    }

    public static void main(String[] args) throws IOException {
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
        FileSystem fileSystem = FileSystem.get(hadoopConfig);
        String filePath = "/hdfsexp/tdrive";
        Path hdfsExpPath = new Path(filePath);
        if (fileSystem.exists(hdfsExpPath)) {
            System.out.println("dir exists");
            fileSystem.delete(hdfsExpPath, true);
        }
        fileSystem.mkdirs(hdfsExpPath);

        StringWriter sw = new StringWriter();
        String[] HEADERS = {"name", "number", "time", "candidates"};
        CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                .setHeader(HEADERS)
                .build();


        int NUM_THREADS = 20;
        var candidates = (new ArrayList<>(inverted.keySet())).stream().sorted().collect(Collectors.toList());
        long start = System.currentTimeMillis();
        List<Result> csvResult = new LinkedList<>();
        for (int i = 0; i < 7001; i=i+1000) {
            System.out.println(i+"-"+(i+1000));
            ExecutorService pool = Executors.newFixedThreadPool(NUM_THREADS);
            for (var name :
                    candidates.subList(i,i+1000)) {
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
                var t = pool.awaitTermination(9999, TimeUnit.DAYS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            csvResult.add(Result.builder()
                            .Name("hdfs-3")
                            .QueryNo(i+1000)
                            .time(System.currentTimeMillis()-start)
                            .TrajSize(inverted.size())
                    .build());
            System.out.println(System.currentTimeMillis()-start);
        }
        ExecutorService pool = Executors.newFixedThreadPool(NUM_THREADS);
        for (var name :
                candidates.subList(8000,candidates.size())) {
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
            var t = pool.awaitTermination(9999, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        csvResult.add(Result.builder()
                .Name("hdfs-3")
                .QueryNo(candidates.size())
                .time(System.currentTimeMillis()-start)
                .TrajSize(inverted.size())
                .build());
        System.out.println(System.currentTimeMillis()-start);
        fileSystem.close();
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
        BufferedWriter writer = new BufferedWriter(new FileWriter("insert-hdfs-3.csv"));
        writer.write(sw.toString());
        writer.close();
    }

    private static void sendMessage(String name, TreeMap<String, List<String>> inverted) throws IOException {
        FileSystem fileSystem = FileSystem.get(hadoopConfig);
        String trajPath = "/hdfsexp/tdrive/" + name + ".txt";
        Path hdfsPath = new Path(trajPath);
        for (var part :
                inverted.get(name)) {
            try (FileReader f = new FileReader("/var/home/liontao/Documents/data/filtered/" + part)) {
                BufferedReader in = new BufferedReader(f);
                String str;
                StringBuilder sb = new StringBuilder();
                while ((str = in.readLine()) != null) {
                    sb.append(str.trim()).append("\n");
                }
                if (fileSystem.exists(hdfsPath)) {
                    FSDataOutputStream fileOutputStream = fileSystem.append(hdfsPath);
                    fileOutputStream.writeBytes(sb.toString());
                    fileOutputStream.hsync();
                    fileOutputStream.close();
                } else {
                    FSDataOutputStream fileOutputStream = fileSystem.create(hdfsPath);
                    fileOutputStream.writeBytes(sb.toString());
                    fileOutputStream.hsync();
                    fileOutputStream.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
    }
}
