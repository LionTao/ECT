package cn.edu.suda.ada.strajdb;

import cn.edu.suda.ada.strajdb.serializers.KryoSerializer;
import cn.edu.suda.ada.strajdb.types.AssemblerActor;
import cn.edu.suda.ada.strajdb.types.AssemblerMsg;
import io.dapr.actors.ActorId;
import io.dapr.actors.client.ActorClient;
import io.dapr.actors.client.ActorProxyBuilder;
import lombok.var;
import org.apache.commons.io.FilenameUtils;
import org.springframework.util.StopWatch;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static cn.edu.suda.ada.strajdb.storage.CatalogUtils.catalogLocal;

/**
 * 测试online插入的动态扩容
 * 主要测试assembler和index在数据量逐渐增加的情况下配合KEDA和Knative进行缩扩容
 */
public class ScaleTest {
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

    public static void main(String[] args) {
        System.out.println(catalogLocal.get().name());
        var stopWatch = new StopWatch("多线程压力测试");
        int NUM_THREADS = 20;
        // 读取数据
        Set<Path> paths = listFilesUsingFilesList("G:/Data/tdrive/filtered");
        // 开线程池, 发送给assembler
        try {
            stopWatch.start("发送tdrive");
            ExecutorService pool = Executors.newFixedThreadPool(NUM_THREADS);
            for (Path p :
                    paths) {
                pool.execute(() -> sendMessage(p));
            }
            pool.shutdown();
            var t = pool.awaitTermination(9999, TimeUnit.DAYS);
            stopWatch.stop();
            System.out.println("\n\n\n\nDone.");
            System.out.println(stopWatch.prettyPrint());
            // 检查所有数据是否已经落盘
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static void sendMessage(Path fp) {
        try (FileReader f = new FileReader(fp.toAbsolutePath().toString());
             ActorClient client = new ActorClient()) {
            ActorProxyBuilder<AssemblerActor> builder = new ActorProxyBuilder<>(AssemblerActor.class, client)
                    .withObjectSerializer(new KryoSerializer());
            ActorId aid = new ActorId(FilenameUtils.removeExtension(fp.getFileName().toString()));
            AssemblerActor actor = builder.build(aid);
            BufferedReader in = new BufferedReader(f);
            String str;
            while ((str = in.readLine()) != null) {
                String[] line = str.trim().split(",");
                if (line.length == 4) {
                    var success = false;
                    while (!success) {
                        try {
                            actor.acceptNewPoint(AssemblerMsg.builder()
                                    .objID(line[0])
                                    .timeStamp(System.currentTimeMillis())
                                    .longitude(Float.parseFloat(line[2]))
                                    .latitude(Float.parseFloat(line[3]))
                                    .build()
                            ).blockOptional();
                            success = true;
                        } catch (Exception e) {
                            System.out.println("retrying");
                        }
                    }
//                    System.out.println(AssemblerMsg.builder()
//                            .objID(line[0])
//                            .timeStamp(System.currentTimeMillis())
//                            .longitude(Float.parseFloat(line[2]))
//                            .latitude(Float.parseFloat(line[3]))
//                            .build().toString());
//                    System.out.println("Sent");
//                    TimeUnit.MILLISECONDS.sleep(6L);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
