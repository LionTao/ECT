package cn.edu.suda.ada.ect.grid;

import cn.edu.suda.ada.ect.types.GridActor;
import cn.edu.suda.ada.ect.types.GridMsg;
import cn.edu.suda.ada.ect.types.ECTActor;
import com.uber.h3core.H3Core;
import io.dapr.actors.ActorId;
import io.dapr.actors.runtime.ActorMethodContext;
import io.dapr.actors.runtime.ActorRuntimeContext;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.util.List;

public class GridActorImpl extends ECTActor implements GridActor {
    private final static H3Core h3core;

    static {
        try {
            h3core = H3Core.newInstance();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private final int resolution;
    private GridActorState state = GridActorState.builder().build();

    /**
     * Instantiates a new Actor.
     *
     * @param runtimeContext Context for the runtime.
     * @param id             Actor identifier.
     */
    public GridActorImpl(ActorRuntimeContext runtimeContext, ActorId id) {
        super(runtimeContext, id);
        this.resolution = h3core.getResolution(id.toString());
    }

    @Override
    protected Mono<Void> onActivate() {
        return super.getActorStateManager()
                .contains("state")
                .flatMap(contains -> {
                    if (contains) {
                        return super.getActorStateManager().get("state", GridActorState.class);
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
        return super.getActorStateManager()
                .set("state", this.state);
    }


    @Override
    public Mono<Boolean> acceptNewGeometry(GridMsg g) {
        if (this.state.isRetired()) {
            logger.warning(this.getId().toString() + " retired!");
            return Mono.just(true);
        } else {
            this.state.addGeometry(g);
            return Mono.just(false);
        }
    }

    @Override
    protected Mono<Void> onPostActorMethod(ActorMethodContext actorMethodContext) {
        if (actorMethodContext.getMethodName().equals("acceptNewGeometry")) {
            if (this.resolution < 15 && (state.getTree().calculateDepth() > 50 || state.getTree().size() > 10000)) {
                this.state.setRetired(true);
            }
        }
        return super.onPostActorMethod(actorMethodContext);
    }

    @Override
    public Mono<List<String>> query(String data) {
        return null;
    }
}
