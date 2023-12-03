package cn.edu.suda.ada.ect.pushdown;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class UploadToHDFS {
    private static final Configuration hadoopConfig = new Configuration();

    static {
        hadoopConfig.set("fs.defaultFS", "131-195:8020");
        hadoopConfig.setBoolean("dfs.client.use.datanode.hostname", true);
        hadoopConfig.set("dfs.replication", "3");
        hadoopConfig.setBoolean("fs.hdfs.impl.disable.cache", true);
        hadoopConfig.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
    }

    public static void main(String[] args) throws IOException {
        FileSystem fileSystem = FileSystem.get(hadoopConfig);
        String trajPath = "/hdfsexp/all.parquet";
        Path hdfsPath = new Path(trajPath);
        try (FileReader f = new FileReader("/var/home/liontao/work/stdtw/all-1.parquet")) {
            BufferedReader in = new BufferedReader(f);
            String str;
            fileSystem.copyFromLocalFile(new Path("/var/home/liontao/work/stdtw/all-1.parquet"), hdfsPath);

        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
