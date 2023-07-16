package cn.edu.suda.ada.strajdb.query;

import com.github.davidmoten.rtree2.geometry.Geometries;
import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.errors.*;
import lombok.Builder;
import lombok.Data;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class MinioWriteBench {
    @Data
    @Builder
    private static class Result {
        int QueryNo;
        int TrajSize;
        long time;
        String Name;
    }

    static String bucketName = "warehouse";

    public static void main(String[] args) throws IOException, ServerException, InsufficientDataException, ErrorResponseException, NoSuchAlgorithmException, InvalidKeyException, InvalidResponseException, XmlParserException, InternalException {
        MinioClient ossClient =
                MinioClient.builder()
                        .endpoint("http://192.168.134.126:9000")
                        .credentials("minioadmin", "minioadmin")
                        .build();
        if (!ossClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build())) {
            ossClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
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


        int NUM_THREADS = 1000;
        var candidates = (new ArrayList<>(inverted.keySet())).stream().sorted().collect(Collectors.toList());
        long start = System.currentTimeMillis();
        List<Result> csvResult = new LinkedList<>();
        for (int i = 0; i < 7001; i = i + 1000) {
            System.out.println(i + "-" + (i + 1000));
            ExecutorService pool = Executors.newFixedThreadPool(NUM_THREADS);
            for (var name :
                    candidates.subList(i, i + 1000)) {
                pool.execute(() -> {
                    try {
                        sendMessage(name, inverted, ossClient);
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
                    .Name("minio")
                    .QueryNo(i + 1000)
                    .time(System.currentTimeMillis() - start)
                    .TrajSize(inverted.size())
                    .build());
            System.out.println(System.currentTimeMillis() - start);

        }
        ExecutorService pool = Executors.newFixedThreadPool(NUM_THREADS);
        for (var name :
                candidates.subList(8000, candidates.size())) {
            pool.execute(() -> {
                try {
                    sendMessage(name, inverted, ossClient);
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
                .Name("minio")
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
        BufferedWriter writer = new BufferedWriter(new FileWriter("insert-minio.csv"));
        writer.write(sw.toString());
        writer.close();
    }

    private static void sendMessage(String name, TreeMap<String, List<String>> inverted, MinioClient ossClient) throws IOException {
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
                // 上传字符串。
                byte[] buffer = sb.toString().getBytes(StandardCharsets.UTF_8);
                ossClient.putObject(PutObjectArgs.builder().bucket("warehouse").object("/filtered/filtered/" + part)
                        .stream(new ByteArrayInputStream(buffer), buffer.length, -1)
                        .build());
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(1);
            } catch (ServerException | InsufficientDataException | ErrorResponseException | NoSuchAlgorithmException |
                     InvalidKeyException | InvalidResponseException | XmlParserException | InternalException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
