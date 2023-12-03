package cn.edu.suda.ada.ect.storage;

import lombok.var;
import org.apache.iceberg.*;
import org.apache.iceberg.aws.AwsClientFactories;
import org.apache.iceberg.aws.AwsClientFactory;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static cn.edu.suda.ada.ect.storage.CatalogUtils.catalogLocal;

public class IcebergUtils {
    private static final Namespace webapp = Namespace.of("ect");
    private static final AwsClientFactory clientFactory = AwsClientFactories.from(getConfig());
    public static final Schema trajectorySegmentSchema = new Schema(
            Types.NestedField.required(1, "trajectory_id", Types.StringType.get()),
            Types.NestedField.required(2, "ts", Types.TimestampType.withoutZone()),
            Types.NestedField.required(3, "lon", Types.FloatType.get()),
            Types.NestedField.required(4, "lat", Types.FloatType.get())
    );

    public static Map<String, String> getConfig() {
        Map<String, String> properties = new HashMap<>();
//        properties.put(CatalogProperties.CATALOG_IMPL, NessieCatalog.class.getName());
//        properties.put(CatalogProperties.URI, "http://localhost:19120/api/v1");
        properties.put(CatalogProperties.CATALOG_IMPL, JdbcCatalog.class.getName());
        properties.put(CatalogProperties.URI, "jdbc:mysql://localhost:3306/demo_catalog");
        properties.put(JdbcCatalog.PROPERTY_PREFIX + "user", "root");
        properties.put(JdbcCatalog.PROPERTY_PREFIX + "password", "password");
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, "s3://warehouse");
        properties.put(CatalogProperties.FILE_IO_IMPL, S3FileIO.class.getName());
        properties.put(AwsProperties.S3FILEIO_ENDPOINT, "http://127.0.0.1:9000");
        properties.put(AwsProperties.HTTP_CLIENT_TYPE, "apache");
        return properties;
    }

    public static void writeTrajectorySegment(String ObjID, List<TrajectoryPointRecord> coors) {
        TableIdentifier name = TableIdentifier.of(webapp, String.format("tdrive_%s", ObjID));
        var catalog = catalogLocal.get();
        Map<String, String> conf = new HashMap<>();
        conf.put(TableProperties.OBJECT_STORE_ENABLED, "true");
        conf.put(TableProperties.WRITE_DATA_LOCATION, "s3://warehouse");
        conf.put(TableProperties.PARQUET_COMPRESSION, "gzip");

        Table tbl;
        try {
            tbl = catalog.createTable(name, trajectorySegmentSchema, PartitionSpec.unpartitioned(), conf);
        } catch (AlreadyExistsException e) {
            tbl = catalog.loadTable(name);
        }
        try (S3FileIO s3FileIO = new S3FileIO(clientFactory::s3)) {
            var outputFile = s3FileIO.newOutputFile("s3://warehouse/data/" + UUID.randomUUID() + "." + ".parquet");
            DataWriter<GenericRecord> dataWriter =
                    Parquet.writeData(outputFile)
                            .schema(trajectorySegmentSchema)
                            .createWriterFunc(GenericParquetWriter::buildWriter)
                            .overwrite()
                            .withSpec(PartitionSpec.unpartitioned())
                            .setAll(conf)
                            .build();

            for (TrajectoryPointRecord c :
                    coors) {
                dataWriter.write(c.toGenericRecord());
            }

            while (true) {
                try {
                    tbl.refresh();
                    tbl.newAppend().appendFile(dataWriter.toDataFile()).commit();
                    break;
                } catch (CommitFailedException e) {
                    TimeUnit.MILLISECONDS.sleep(10L);
                }
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
