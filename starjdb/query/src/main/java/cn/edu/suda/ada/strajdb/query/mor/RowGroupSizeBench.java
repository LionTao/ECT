package cn.edu.suda.ada.strajdb.query.mor;

import lombok.Builder;
import lombok.Data;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import java.io.IOException;

public class RowGroupSizeBench {
    @Data
    @Builder
    private static class Result {
        int RowGroupNumber;
        int RowgroupSize;
        long time;
    }

    private static final Configuration hadoopConfig = new Configuration();

    static {
        hadoopConfig.set("fs.alluxio.impl", "alluxio.hadoop.FileSystem");
        hadoopConfig.set("fs.AbstractFileSystem.alluxio.impl", "alluxio.hadoop.AlluxioFileSystem");
        hadoopConfig.set("fs.s3a.access.key", "minioadmin");
        hadoopConfig.set("fs.s3a.secret.key", "minioadmin");
        hadoopConfig.set("fs.s3a.endpoint", "http://192.168.134.126:9000");
    }

    public static void main(String[] args) throws IOException {
    }

    public static Result worker(String filename) throws IOException {
        var p = new Path("s3a://warehouse/" + filename);
        try (ParquetFileReader r = ParquetFileReader.open(HadoopInputFile.fromPath(p, hadoopConfig), ParquetReadOptions.builder().useStatsFilter().build())) {
            System.out.println(r.getRowGroups().size());
        }
//        FilterPredicate predicate = FilterApi.eq(FilterApi.binaryColumn("id"), Binary.fromCharSequence(new Utf8("9999")));
        var predicate = FilterApi.eq(FilterApi.floatColumn("lat"), 39.99651f);
        var start = System.currentTimeMillis();
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
        return null;
    }

}
