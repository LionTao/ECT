package cn.edu.suda.ada.strajdb.storage;

import com.google.common.collect.ImmutableMap;
import lombok.Builder;
import lombok.Value;
import org.apache.iceberg.data.GenericRecord;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.TimeZone;

@Value
@Builder
public class TrajectoryPointRecord {
    String objID;
    long ts;
    float lon;
    float lat;

    public GenericRecord toGenericRecord(){
        GenericRecord record = GenericRecord.create(IcebergUtils.trajectorySegmentSchema);
        return record.copy(ImmutableMap.of(
                "trajectory_id", objID,
                "ts", LocalDateTime.ofInstant(Instant.ofEpochMilli(ts),
                        ZoneOffset.UTC),
                "lon", lon,
                "lat", lat
        ));
    }
}
