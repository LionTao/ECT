package cn.edu.suda.ada.strajdb.assembler;

import cn.edu.suda.ada.strajdb.serializers.KryoSerializer;
import cn.edu.suda.ada.strajdb.storage.TrajectoryPointRecord;
import cn.edu.suda.ada.strajdb.types.*;
import com.uber.h3core.H3Core;
import io.dapr.actors.ActorId;
import io.dapr.actors.client.ActorClient;
import io.dapr.actors.client.ActorProxyBuilder;
import io.dapr.actors.runtime.ActorMethodContext;
import io.dapr.actors.runtime.ActorRuntimeContext;
import lombok.var;
import org.locationtech.jts.geom.Polygon;
import reactor.core.publisher.Mono;

import java.io.IOException;

import static cn.edu.suda.ada.strajdb.storage.IcebergUtils.writeTrajectorySegment;

public class AssemblerActorImpl extends StrajDBActor implements AssemblerActor {
    private final static ActorClient client = new ActorClient();
    private final static ActorProxyBuilder<GridActor> gridBuilder = new ActorProxyBuilder<>(GridActor.class, client)
            .withObjectSerializer(new KryoSerializer());
    private final static H3Core h3core;

    static {
        try {
            h3core = H3Core.newInstance();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private AssemblerActorState state = AssemblerActorState.builder().build();

    /**
     * Instantiates a new Actor.
     *
     * @param runtimeContext Context for the runtime.
     * @param id             Actor identifier.
     */
    public AssemblerActorImpl(ActorRuntimeContext runtimeContext, ActorId id) {
        super(runtimeContext, id);
    }

    @Override
    protected Mono<Void> onActivate() {
        return super.getActorStateManager()
                .contains("state")
                .flatMap(contains -> {
                    if (contains) {
                        return super.getActorStateManager().get("state", AssemblerActorState.class);
                    } else {
                        return Mono.just(this.state);
                    }
                }).flatMap(s -> {
                    this.state = s;
                    return Mono.empty();
                });
    }

    @Override
    protected Mono<Void> onDeactivate() {
        return sendGridMsgWithRetry()
                .flatMap(c-> super.getActorStateManager()
                        .set("state", this.state));
    }

    /**
     * 轨迹组装与分段
     * 分段前以点的形式在缓冲中
     * 查询轨迹需要同时查询iceberg和缓冲
     *
     * @param msg 轨迹数据
     */
    @Override
    public Mono<Void> acceptNewPoint(AssemblerMsg msg) {
        // 保留meta和cursor为轨迹切断做准备
        this.state = this.state.addPoint(TrajectoryPointRecord.builder()
                .ts(msg.getTimeStamp())
                .objID(this.getId().toString())
                .lat(msg.getLatitude())
                .lon(msg.getLongitude())
                .build());
        return Mono.empty();
    }

    @Override
    protected Mono<Void> onPostActorMethod(ActorMethodContext actorMethodContext) {
        if (actorMethodContext.getMethodName().equals("acceptNewPoint")) {
            // 判断切断
            long expire = System.currentTimeMillis() - this.state.lastTime;
            if ((expire > 5000 || this.state.timeSpan > 10000) && this.state.getSegment().size() > 2) {
                logger.info("发送");
                return sendGridMsgWithRetry();
            }
        }
        return super.onPostActorMethod(actorMethodContext);
    }

    private Mono<Void> sendGridMsgWithRetry() {
        // 切断(FIXME:选择哪些分区发送？)
        ActorId aid = new ActorId(h3core.latLngToCellAddress(
                this.state.getTrajectorySegment().getInteriorPoint().getY(),
                this.state.getTrajectorySegment().getInteriorPoint().getX(),
                this.state.getResolution()
        ));
        GridActor gridActor = gridBuilder.build(aid);
        try {
            var gridMsg = GridMsg.builder()
                    .objID(this.getId().toString())
                    .mbr((Polygon) this.state.getTrajectorySegment().buffer(0.00001).getEnvelope())
                    .build();
            return gridActor.acceptNewGeometry(gridMsg)
                    .flatMap(retired -> {
                        if (retired && this.state.getResolution() < 15) {
                            // 增加resolution并清空状态
                            this.state = this.state.addRes();
                            return sendGridMsgWithRetry();
                        }
                        return Mono.just(this.state.getSegment())
                                .flatMap(segment -> {
                                    writeTrajectorySegment(this.getId().toString(), this.state.getSegment());
                                    this.state = AssemblerActorState.builder().resolution(this.state.getResolution()).build();
                                    return Mono.empty();
                                });
                    });
        } catch (ClassCastException e) {
            e.printStackTrace();
            logger.warning(this.state.getSegment().toString());
            return Mono.empty();
        }

    }
}
