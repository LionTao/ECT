package cn.edu.suda.ada.strajdb.query.ch5;

import lombok.Builder;
import lombok.Data;
import lombok.var;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.*;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class PushDownHBaseID {

    @Data
    @Builder
    private static class Result {
        int QueryNo;
        double Traffic;
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
        List<Result> csvResult = new LinkedList<>();
        int NUM = 2;
        // HBase
        var start = System.currentTimeMillis();
        var startRx = RxMB();
        for (int i = 1; i < NUM; i++) {
            Table t = connection.getTable(TableName.valueOf(String.valueOf(i)));
            var scan = new Scan();
            var scanner = t.getScanner(scan);
            var sb = new StringBuilder();
            for (Iterator<org.apache.hadoop.hbase.client.Result> it = scanner.iterator(); it.hasNext(); ) {
                org.apache.hadoop.hbase.client.Result temp = it.next();
                if (temp!=null){
                    sb.append(temp);
                    System.out.println(temp);
                }
            }
            var length = sb.toString().length();
            var endRx = RxMB() - startRx;
            var push = System.currentTimeMillis() - start;
            csvResult.add(Result.builder()
                    .QueryNo(i)
                    .Name("hbase")
                    .time(push)
                    .Traffic(endRx)
                    .build());
        }
        writeCSV(csvResult, "pushdown-id-hbase.csv");
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
