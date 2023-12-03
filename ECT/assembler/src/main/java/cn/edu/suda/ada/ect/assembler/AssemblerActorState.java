package cn.edu.suda.ada.ect.assembler;

import cn.edu.suda.ada.ect.storage.TrajectoryPointRecord;
import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.KryoSerializable;
import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;
import lombok.*;
import org.apache.commons.lang3.NotImplementedException;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;

import java.util.LinkedList;
import java.util.stream.Collectors;

@Data
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
@Builder(toBuilder = true)
@ToString
public class AssemblerActorState implements KryoSerializable {
    private final static GeometryFactory factory = JTSFactoryFinder.getGeometryFactory();

    @Builder.Default
    LinkedList<TrajectoryPointRecord> segment = new LinkedList<>();

    @Builder.Default
    int resolution = 0;

    @Builder.Default
    long lastTime = System.currentTimeMillis();

    @Builder.Default
    long timeSpan = 0L;

    /**
     * 添加一个点，返回新的状态
     *
     * @return
     */
    public AssemblerActorState addPoint(TrajectoryPointRecord e) {
        segment.add(e);
        if (this.lastTime == -1L) {
            lastTime = e.getTs();
            timeSpan = 0;
        } else {
            timeSpan = this.getTimeSpan() + e.getTs() - this.getLastTime();
            lastTime = e.getTs();
        }
        return this;
    }

    public LineString getTrajectorySegment() {
        if (this.segment.size()<3){
            return null;
        }
        Coordinate[] myArray = new Coordinate[this.segment.size()];
        this.segment.stream().map(p -> new Coordinate(p.getLon(), p.getLat())).collect(Collectors.toList()).toArray(myArray);
        return new LineString(new CoordinateArraySequence(myArray), factory);
    }

    /**
     * 添加一个点，返回新的状态
     *
     * @return
     */
    public AssemblerActorState addRes() {
        this.setResolution(this.resolution + 1);
        return this;
    }

    public double getLength() {
        throw new NotImplementedException("TODO");
    }

    @Override
    public void write(Kryo kryo, Output output) {
        kryo.writeObject(output, this);
    }

    @Override
    public void read(Kryo kryo, Input input) {
        var temp = kryo.readObject(input, this.getClass());
        this.setSegment(temp.getSegment());
        this.setResolution(temp.resolution);
        this.setLastTime(temp.getLastTime());
        this.setTimeSpan(temp.getTimeSpan());
    }
}
