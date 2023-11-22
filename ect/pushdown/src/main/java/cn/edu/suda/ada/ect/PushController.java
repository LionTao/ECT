package cn.edu.suda.ada.ect;

import lombok.var;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
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
import org.apache.parquet.io.api.Binary;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@RestController
public class PushController {
    private static final Configuration hadoopConfig = new Configuration();
    private static final Path p = new Path("s3a://warehouse/all.parquet");

    static {
        hadoopConfig.set("fs.alluxio.impl", "alluxio.hadoop.FileSystem");
        hadoopConfig.set("fs.AbstractFileSystem.alluxio.impl", "alluxio.hadoop.AlluxioFileSystem");
        hadoopConfig.set("fs.s3a.access.key", "minioadmin");
        hadoopConfig.set("fs.s3a.secret.key", "minioadmin");
        hadoopConfig.set("fs.s3a.endpoint", "http://192.168.134.126:9000");
    }

    @GetMapping("/test")
    public String test() {
        return "hello";
    }

    @GetMapping("/id")
    @ResponseBody
    public List<String> getids(@RequestParam String id) throws IOException {
        try (ParquetFileReader r = ParquetFileReader.open(HadoopInputFile.fromPath(p, hadoopConfig), ParquetReadOptions.builder().useStatsFilter().build())) {
            System.out.println(r.getRowGroups().size());
        }
        FilterPredicate predicate = FilterApi.eq(FilterApi.binaryColumn("id"), Binary.fromCharSequence(new Utf8(id)));
//        var predicate = FilterApi.eq(FilterApi.floatColumn("lat"), 39.99651f);
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(HadoopInputFile.fromPath(p, hadoopConfig))
                .withFilter(FilterCompat.get(predicate))
                .useRecordFilter()
                .build()) {
            var res = new ArrayList<String>();
            GenericRecord nextRecord = reader.read();
            while (nextRecord != null) {
                res.add(nextRecord.toString());
                nextRecord = reader.read();
            }
            return res;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @GetMapping("/idtime")
    @ResponseBody
    public List<String> getIDTime(@RequestParam String id, @RequestParam long minT, @RequestParam long maxT) throws IOException {
        try (ParquetFileReader r = ParquetFileReader.open(HadoopInputFile.fromPath(p, hadoopConfig), ParquetReadOptions.builder().useStatsFilter().build())) {
            System.out.println(r.getRowGroups().size());
        }
//        FilterPredicate predicate = FilterApi.eq(FilterApi.binaryColumn("id"), Binary.fromCharSequence(new Utf8(id)));
        var predicate = FilterApi.and(
                FilterApi.eq(FilterApi.binaryColumn("id"), Binary.fromCharSequence(new Utf8(id))),
                FilterApi.and(
                        FilterApi.ltEq(FilterApi.longColumn("ts"), maxT),
                        FilterApi.gtEq(FilterApi.longColumn("ts"), minT)
                )
        );
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(HadoopInputFile.fromPath(p, hadoopConfig))
                .withFilter(FilterCompat.get(predicate))
                .useRecordFilter()
                .build()) {
            var res = new ArrayList<String>();
            GenericRecord nextRecord = reader.read();
            while (nextRecord != null) {
                res.add(nextRecord.toString());
                nextRecord = reader.read();
            }
            return res;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @GetMapping("/time")
    @ResponseBody
    public List<String> getTime(@RequestParam long minT, @RequestParam long maxT) throws IOException {
        try (ParquetFileReader r = ParquetFileReader.open(HadoopInputFile.fromPath(p, hadoopConfig), ParquetReadOptions.builder().useStatsFilter().build())) {
            System.out.println(r.getRowGroups().size());
        }
//        FilterPredicate predicate = FilterApi.eq(FilterApi.binaryColumn("id"), Binary.fromCharSequence(new Utf8(id)));
        var predicate = FilterApi.and(
                FilterApi.ltEq(FilterApi.longColumn("ts"), maxT),
                FilterApi.gtEq(FilterApi.longColumn("ts"), minT)
        );
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(HadoopInputFile.fromPath(p, hadoopConfig))
                .withFilter(FilterCompat.get(predicate))
                .useRecordFilter()
                .build()) {
            var res = new ArrayList<String>();
            GenericRecord nextRecord = reader.read();
            while (nextRecord != null) {
                res.add(nextRecord.toString());
                nextRecord = reader.read();
            }
            return res;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @GetMapping("/spatial")
    @ResponseBody
    public List<String> getSpatial(@RequestParam float minX, @RequestParam float minY, @RequestParam float maxX, @RequestParam float maxY) throws IOException {
        try (ParquetFileReader r = ParquetFileReader.open(HadoopInputFile.fromPath(p, hadoopConfig), ParquetReadOptions.builder().useStatsFilter().build())) {
            System.out.println(r.getRowGroups().size());
        }
//        FilterPredicate predicate = FilterApi.eq(FilterApi.binaryColumn("id"), Binary.fromCharSequence(new Utf8(id)));
        var predicate = FilterApi.and(
                FilterApi.and(
                        FilterApi.gtEq(FilterApi.floatColumn("lon"), minX),
                        FilterApi.ltEq(FilterApi.floatColumn("lon"), maxX)
                ),
                FilterApi.and(
                        FilterApi.gtEq(FilterApi.floatColumn("lat"), minY),
                        FilterApi.ltEq(FilterApi.floatColumn("lat"), maxY)
                )
        );
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(HadoopInputFile.fromPath(p, hadoopConfig))
                .withFilter(FilterCompat.get(predicate))
                .useRecordFilter()
                .build()) {
            var res = new ArrayList<String>();
            GenericRecord nextRecord = reader.read();
            while (nextRecord != null) {
                var id = nextRecord.get("id");
                var ts = nextRecord.get("ts");
                var lon = nextRecord.get("lon");
                var lat = nextRecord.get("lat");
                res.add(id+","+ts+","+lon+","+lat);
                nextRecord = reader.read();
            }
            return res;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @GetMapping("/idspatial")
    @ResponseBody
    public List<String> getIDSpatial(@RequestParam String id, @RequestParam float minX, @RequestParam float minY, @RequestParam float maxX, @RequestParam float maxY) throws IOException {
        try (ParquetFileReader r = ParquetFileReader.open(HadoopInputFile.fromPath(p, hadoopConfig), ParquetReadOptions.builder().useStatsFilter().build())) {
            System.out.println(r.getRowGroups().size());
        }
//        FilterPredicate predicate = FilterApi.eq(FilterApi.binaryColumn("id"), Binary.fromCharSequence(new Utf8(id)));
        var predicate = FilterApi.and(
                FilterApi.eq(FilterApi.binaryColumn("id"), Binary.fromCharSequence(new Utf8(id))),
                FilterApi.and(
                        FilterApi.and(
                                FilterApi.gtEq(FilterApi.floatColumn("lon"), minX),
                                FilterApi.ltEq(FilterApi.floatColumn("lon"), maxX)
                        ),
                        FilterApi.and(
                                FilterApi.gtEq(FilterApi.floatColumn("lat"), minY),
                                FilterApi.ltEq(FilterApi.floatColumn("lat"), maxY)
                        )
                )
        );
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(HadoopInputFile.fromPath(p, hadoopConfig))
                .withFilter(FilterCompat.get(predicate))
                .useRecordFilter()
                .build()) {
            var res = new ArrayList<String>();
            GenericRecord nextRecord = reader.read();
            while (nextRecord != null) {
                res.add(nextRecord.toString());
                nextRecord = reader.read();
            }
            return res;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @GetMapping("/st")
    @ResponseBody
    public List<String> getST(@RequestParam String[] id, @RequestParam long minT, @RequestParam long maxT, @RequestParam float minX, @RequestParam float minY, @RequestParam float maxX, @RequestParam float maxY) throws IOException {
        try (ParquetFileReader r = ParquetFileReader.open(HadoopInputFile.fromPath(p, hadoopConfig), ParquetReadOptions.builder().useStatsFilter().build())) {
            System.out.println(r.getRowGroups().size());
        }
        var predicate = FilterApi.and(
                FilterApi.and(
                        FilterApi.ltEq(FilterApi.longColumn("ts"), maxT),
                        FilterApi.gtEq(FilterApi.longColumn("ts"), minT)
                ),
                FilterApi.and(
                        FilterApi.and(
                                FilterApi.gtEq(FilterApi.floatColumn("lon"), minX),
                                FilterApi.ltEq(FilterApi.floatColumn("lon"), maxX)
                        ),
                        FilterApi.and(
                                FilterApi.gtEq(FilterApi.floatColumn("lat"), minY),
                                FilterApi.ltEq(FilterApi.floatColumn("lat"), maxY)
                        )
                )
        );
        Set<Binary> idList = Arrays.stream(id).map(x -> Binary.fromCharSequence(new Utf8(x))).collect(Collectors.toSet());
        predicate = FilterApi.and(
                FilterApi.in(FilterApi.binaryColumn("id"), idList), predicate);
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(HadoopInputFile.fromPath(p, hadoopConfig))
                .withFilter(FilterCompat.get(predicate))
                .useRecordFilter()
                .build()) {
            var res = new ArrayList<String>();
            GenericRecord nextRecord = reader.read();
            while (nextRecord != null) {
                res.add(nextRecord.toString());
                nextRecord = reader.read();
            }
            return res;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
