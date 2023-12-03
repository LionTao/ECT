package cn.edu.suda.ada.ect.query.mor;

import io.minio.GetObjectArgs;
import io.minio.MinioClient;
import io.minio.errors.*;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import java.io.*;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

public class MOR {
    private static final Configuration hadoopConfig = new Configuration();

    static {
        hadoopConfig.set("fs.alluxio.impl", "alluxio.hadoop.FileSystem");
        hadoopConfig.set("fs.AbstractFileSystem.alluxio.impl", "alluxio.hadoop.AlluxioFileSystem");
        hadoopConfig.set("fs.s3a.access.key", "minioadmin");
        hadoopConfig.set("fs.s3a.secret.key", "minioadmin");
        hadoopConfig.set("fs.s3a.endpoint", "http://192.168.134.126:9000");
    }

    public static void main(String[] args) throws IOException {
        var p = new Path("alluxio://localhost:19998/all.parquet");
        try (ParquetFileReader r = ParquetFileReader.open(HadoopInputFile.fromPath(p, hadoopConfig), ParquetReadOptions.builder().useStatsFilter().build())) {
            System.out.println(r.getRowGroups().size());
        }
//        FilterPredicate predicate = FilterApi.eq(FilterApi.binaryColumn("id"), Binary.fromCharSequence(new Utf8("9999")));
        FilterPredicate predicate = FilterApi.eq(FilterApi.floatColumn("lat"), 39.99651f);
        long start = System.currentTimeMillis();
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(HadoopInputFile.fromPath(p, hadoopConfig))
                .withFilter(FilterCompat.get(predicate))
                .useBloomFilter()
                .useRecordFilter()
                .build()) {
            GenericRecord nextRecord = reader.read();
            while (nextRecord != null) {
                System.out.println(nextRecord);
                nextRecord = reader.read();
            }
        }
        var push = System.currentTimeMillis() - start;

        p = new Path("s3a://warehouse/all.parquet");
        try (ParquetFileReader r = ParquetFileReader.open(HadoopInputFile.fromPath(p, hadoopConfig), ParquetReadOptions.builder().useStatsFilter().build())) {
            System.out.println(r.getRowGroups().size());
        }
//        FilterPredicate predicate = FilterApi.eq(FilterApi.binaryColumn("id"), Binary.fromCharSequence(new Utf8("9999")));
        predicate = FilterApi.eq(FilterApi.floatColumn("lat"), 39.99651f);
        start = System.currentTimeMillis();
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(HadoopInputFile.fromPath(p, hadoopConfig))
                .withFilter(FilterCompat.get(predicate))
                .useRecordFilter()
                .build()) {
            GenericRecord nextRecord = reader.read();
            while (nextRecord != null) {
                System.out.println(nextRecord);
                nextRecord = reader.read();
            }
        }
        var push2 = System.currentTimeMillis() - start;


        start = System.currentTimeMillis();
        try (FileReader f = new FileReader("/var/home/liontao/work/stdtw/all.csv")) {
            BufferedReader in = new BufferedReader(f);
            String str;
            while ((str = in.readLine()) != null) {
                String[] record = str.split(",");
                try {
//                    if (Objects.equals(record[0], "\"9999\"")){
//                        System.out.println(str);
//                    }
                    if (Objects.equals(record[3], "39.99651")) {
                        System.out.println(str);
                    }
                } catch (Exception ignored) {

                }
            }
        }
        var raw = System.currentTimeMillis() - start;

        MinioClient minioClient =
                MinioClient.builder()
                        .endpoint("http://192.168.134.126:9000")
                        .credentials("minioadmin", "minioadmin")
                        .build();
        start = System.currentTimeMillis();
        try (InputStream in = minioClient.getObject(
                GetObjectArgs.builder()
                        .bucket("warehouse")
                        .object("all.csv")
                        .build())) {
            // Read data from stream
            try (BufferedReader br
                         = new BufferedReader(new InputStreamReader(in))) {
                String str;
                while ((str = br.readLine()) != null) {
                    String[] record = str.split(",");
                    try {
//                        if (Objects.equals(record[0], "\"9999\"")){
//                            System.out.println(str);
//                        }
                        if (Objects.equals(record[3], "39.99651")) {
                            System.out.println(str);
                        }
                    } catch (Exception ignored) {

                    }
                }
            }
        } catch (ServerException | InsufficientDataException | ErrorResponseException | NoSuchAlgorithmException |
                 InvalidKeyException | InvalidResponseException | XmlParserException | InternalException e) {
            throw new RuntimeException(e);
        }
        var minio = System.currentTimeMillis() - start;
        System.out.println(raw);
        System.out.printf("minio: %d\npush: %d\npush: %d\nacc: %f", minio, push, push2, (double) (minio - push) * 100 / minio);
        // no cache lat == 39.99651
//        minio: 71206
//        push: 1326
//        acc: 98.137797
        // no cache lat == 39.99651
//        raw:3101
//        minio: 70558
//        push: 1244
//        push2-no-cache: 50966
//        acc: 98.236911
        // cached id==10357
//        minio: 70355
//        push: 416
//        acc: 99.408713

//         cached id==9999
//        raw:2598
//        minio: 71136
//        push: 393
//        acc: 99.447537
    }
}
