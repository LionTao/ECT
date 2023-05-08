package cn.edu.suda.ada.strajdb.serializers;

import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.io.ByteBufferInput;
import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;
import com.fasterxml.jackson.databind.type.TypeFactory;
import io.dapr.serializer.DaprObjectSerializer;
import io.dapr.utils.TypeRef;
import lombok.var;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.logging.Logger;

import static cn.edu.suda.ada.strajdb.serializers.KryoUtils.kryoLocal;

public class KryoSerializer implements DaprObjectSerializer {
    Logger logger = Logger.getLogger(this.getClass().getName());

    @Override
    public byte[] serialize(Object o) throws IOException {
        try (Output output = new Output(new ByteArrayOutputStream())) {
            kryoLocal.get().writeObject(output, o);
            return output.toBytes();
        } catch (Exception e) {
            logger.warning("Dapr serialize failed!");
            e.printStackTrace();
            throw e;
        }
    }


    @Override
    @SuppressWarnings("unchecked")
    public <T> T deserialize(byte[] bytes, TypeRef<T> typeRef) throws IOException {

        try (Input input = new Input(new ByteBufferInput(bytes))) {
            Kryo kryo = kryoLocal.get();
            var c = (Class<T>) TypeFactory.defaultInstance().constructType(typeRef.getType()).getRawClass();
            return kryo.readObject(input, c);
        } catch (Exception e) {
            logger.warning("Dapr deserialize failed!");
            e.printStackTrace();
            throw e;
        }
    }

    @Override
    public String getContentType() {
        return "application/json";
    }
}
