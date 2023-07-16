package query.ch5;

import lombok.var;
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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PushDownHBaseTable {
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
        var name = "pushdown";
        var admin = connection.getAdmin();
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
        for (var part :
                listFilesUsingFilesList("/var/home/liontao/Downloads/tdrive")) {
            try (FileReader f = new FileReader(part.toFile())) {
                BufferedReader in = new BufferedReader(f);
                String str;
//                StringBuilder sb = new StringBuilder();
                List<Put> puts = new ArrayList<>();
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
                    puts.add(put1);
                }
                t.put(puts);
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
        t.close();
        admin.close();
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
}
