package cn.edu.suda.ada.ect.types;

import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.KryoSerializable;
import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;
import lombok.*;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
@ToString
public class GridMsg implements KryoSerializable {
    String objID;
    Polygon mbr;

    @Override
    public void write(Kryo kryo, Output output) {
        output.writeString(this.getObjID());
        var wkb = new WKBWriter().write(this.getMbr());
        output.writeInt(wkb.length);
        output.write(wkb);
    }

    @Override
    public void read(Kryo kryo, Input input) {
        this.setObjID(input.readString());
        var wkbLength = input.readInt();
        try {
            this.setMbr((Polygon) new WKBReader().read(input.readBytes(wkbLength)));
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }
}
