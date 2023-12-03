package cn.edu.suda.ada.ect.grid;

import cn.edu.suda.ada.ect.types.GridMsg;
import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.KryoSerializable;
import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;
import com.github.davidmoten.rtree.InternalStructure;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.Serializer;
import com.github.davidmoten.rtree.Serializers;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Rectangle;
import lombok.*;
import org.locationtech.jts.geom.Coordinate;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
@EqualsAndHashCode
@ToString
public class GridActorState implements KryoSerializable {
    private final Serializer<String, Rectangle> SERIALIZER = Serializers.flatBuffers().utf8();

    @Builder.Default
    private RTree<String, Rectangle> tree = RTree.star().minChildren(4).maxChildren(23).create();

    @Builder.Default
    private boolean isRetired = false;

    public void addGeometry(GridMsg m) {
        double minLon = Float.POSITIVE_INFINITY;
        double minLat = Float.POSITIVE_INFINITY;
        double maxLon = Float.NEGATIVE_INFINITY;
        double maxLat = Float.NEGATIVE_INFINITY;
        for (Coordinate coordinate : m.getMbr().getCoordinates()) {
            minLon = Double.min(coordinate.x, minLon);
            minLat = Double.min(coordinate.y, minLat);
            maxLon = Double.max(coordinate.x, maxLon);
            maxLat = Double.max(coordinate.y, maxLat);
        }
        this.setTree(this.getTree().add(
                m.getObjID(),
                Geometries.rectangleGeographic(minLon, minLat, maxLon, maxLat)
        ));
    }

    @Override
    public void write(Kryo kryo, Output output) {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        try {
            SERIALIZER.write(tree, os);
            os.flush();
            os.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        var res = os.toByteArray();
        output.writeInt(res.length);
        output.writeBytes(res);
        output.writeBoolean(isRetired);
    }

    @Override
    public void read(Kryo kryo, Input input) {
        var treeLength = input.readInt();
        var serializedTree = input.readBytes(treeLength);
        var is = new ByteArrayInputStream(serializedTree);
        try {
            this.setTree(SERIALIZER.read(is, treeLength, InternalStructure.DEFAULT));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.isRetired = input.readBoolean();
    }
}
