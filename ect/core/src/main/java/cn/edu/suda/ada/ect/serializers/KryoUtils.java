package cn.edu.suda.ada.ect.serializers;

import cn.edu.suda.ada.ect.types.AssemblerMsg;
import cn.edu.suda.ada.ect.types.GridMsg;
import com.esotericsoftware.kryo.kryo5.Kryo;

public class KryoUtils {

    final public static ThreadLocal<Kryo> kryoLocal = ThreadLocal.withInitial(() -> {
        Kryo kryo = new Kryo();
        kryo.setClassLoader(Thread.currentThread().getContextClassLoader());
        kryo.setRegistrationRequired(false);
        // 提前注册好需要处理的序列化对象
        kryo.register(AssemblerMsg.class);
        kryo.register(GridMsg.class);
        return kryo;
    });

    private KryoUtils() {
    }
}
