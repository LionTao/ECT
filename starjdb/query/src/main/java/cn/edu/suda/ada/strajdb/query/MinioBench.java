package cn.edu.suda.ada.strajdb.query;

import alluxio.exception.AlluxioException;
import io.minio.GetObjectArgs;
import io.minio.MinioClient;
import io.minio.errors.*;
import org.springframework.util.StopWatch;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MinioBench {
    public static void main(String[] args) {
        StopWatch stopWatch = new StopWatch("多线程压力测试");
        int NUM_THREADS = 30;
        Set<Path> paths = listFilesUsingFilesList("/var/home/liontao/Documents/data/filtered");
        System.out.println(paths.size());
        // 开线程池, 发送给assembler
        try {
            stopWatch.start("发送tdrive");
            ExecutorService pool = Executors.newFixedThreadPool(NUM_THREADS);
            for (int i = 0; i < 30; i++) {

                pool.execute(() -> {
                    try {
                        readTrajs(new ArrayList<>(paths));
                    } catch (IOException | AlluxioException e) {
                        throw new RuntimeException(e);
                    }
                });

            }
            pool.shutdown();
            boolean t = pool.awaitTermination(9999, TimeUnit.DAYS);
            stopWatch.stop();
            System.out.println("\n\n\n\nDone.");
            System.out.println(stopWatch.prettyPrint());
            // 检查所有数据是否已经落盘
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
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

    public static void readTrajs(List<Path> paths) throws IOException, AlluxioException {
        MinioClient minioClient =
                MinioClient.builder()
                        .endpoint("http://192.168.134.126:9000")
                        .credentials("LTAI5tFHYLvu5Vd5sV7ttCE7", "np1bhVbVSTaVT9uWeYJbpeiuWKZCbV")
                        .build();
        int K = 200;
        Collections.shuffle(paths);
        List<Path> targets = paths.subList(0, K);
        for (Path p :
                targets.subList(0, targets.size())) {
            try (InputStream in = minioClient.getObject(
                    GetObjectArgs.builder()
                            .bucket("warehouse")
                            .object("filtered/filtered/" + p.getFileName())
                            .build())) {
                // Read data from stream
                StringBuilder resultStringBuilder = new StringBuilder();
                try (BufferedReader br
                             = new BufferedReader(new InputStreamReader(in))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        resultStringBuilder.append(line).append("\n");
                    }
                }
                int t = resultStringBuilder.toString().length();
            } catch (ServerException | InsufficientDataException | ErrorResponseException | NoSuchAlgorithmException |
                     InvalidKeyException | InvalidResponseException | XmlParserException | InternalException e) {
                throw new RuntimeException(e);
            }

        }
    }
}
