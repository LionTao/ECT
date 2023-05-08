package cn.edu.suda.ada.strajdb.query;

import com.github.davidmoten.rtree2.geometry.Geometries;
import lombok.Builder;
import lombok.Data;
import lombok.var;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.PutObjectRequest;
import com.aliyun.oss.model.PutObjectResult;
import java.io.ByteArrayInputStream;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class OSSBench {
    @Data
    @Builder
    private static class Result{
        int QueryNo;
        int TrajSize;
        long time;
        String Name;
    }
    // Endpoint以华东1（杭州）为例，其它Region请按实际情况填写。
    static String endpoint = "https://oss-cn-shanghai.aliyuncs.com";
    // 阿里云账号AccessKey拥有所有API的访问权限，风险很高。强烈建议您创建并使用RAM用户进行API访问或日常运维，请登录RAM控制台创建RAM用户。
    static String accessKeyId = "LTAI5tFHYLvu5Vd5sV7ttCE7";
    static String accessKeySecret = "np1bhVbVSTaVT9uWeYJbpeiuWKZCbV";
    // 填写Bucket名称，例如examplebucket。
    static String bucketName = "liontao-thesis";
    public static void main(String[] args) throws IOException {
        OSS ossClient = new OSSClientBuilder().build(endpoint, accessKeyId, accessKeySecret);
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


        int NUM_THREADS = 1000;
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
                        sendMessage(name, inverted,ossClient);
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
        BufferedWriter writer = new BufferedWriter(new FileWriter("insert-oss.csv"));
        writer.write(sw.toString());
        writer.close();
        if (ossClient != null) {
            ossClient.shutdown();
        }
    }

    private static void sendMessage(String name, TreeMap<String, List<String>> inverted,OSS ossClient) throws IOException {
        for (var part :
                inverted.get(name)) {

            try (FileReader f = new FileReader("/var/home/liontao/Documents/data/filtered/" + part)) {
                BufferedReader in = new BufferedReader(f);
                String str;
                StringBuilder sb = new StringBuilder();
                while ((str = in.readLine()) != null) {
                    sb.append(str.trim()).append("\n");
                }
                //write
                // 如果需要上传时设置存储类型和访问权限，请参考以下示例代码。
                // ObjectMetadata metadata = new ObjectMetadata();
                // metadata.setHeader(OSSHeaders.OSS_STORAGE_CLASS, StorageClass.Standard.toString());
                // metadata.setObjectAcl(CannedAccessControlList.Private);
                // putObjectRequest.setMetadata(metadata);
                // 创建PutObjectRequest对象。
                PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, "thesis/"+UUID.randomUUID(), new ByteArrayInputStream(sb.toString().getBytes(StandardCharsets.UTF_8)));
                putObjectRequest.setCallback(null);
                putObjectRequest.setProcess(null);
                // 上传字符串。
                ossClient.putObject(putObjectRequest);
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(1);
            } catch (OSSException oe) {
                System.out.println("Caught an OSSException, which means your request made it to OSS, "
                        + "but was rejected with an error response for some reason.");
                System.out.println("Error Message:" + oe.getErrorMessage());
                System.out.println("Error Code:" + oe.getErrorCode());
                System.out.println("Request ID:" + oe.getRequestId());
                System.out.println("Host ID:" + oe.getHostId());
                System.exit(1);
            } catch (ClientException ce) {
                System.out.println("Caught an ClientException, which means the client encountered "
                        + "a serious internal problem while trying to communicate with OSS, "
                        + "such as not being able to access the network.");
                System.out.println("Error Message:" + ce.getMessage());
                System.exit(1);
            }
        }
    }
}
