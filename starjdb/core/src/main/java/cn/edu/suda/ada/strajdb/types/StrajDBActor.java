package cn.edu.suda.ada.strajdb.types;

import io.dapr.actors.ActorId;
import io.dapr.actors.runtime.AbstractActor;
import io.dapr.actors.runtime.ActorRuntimeContext;

import java.util.logging.Logger;

public abstract class StrajDBActor extends AbstractActor {
    protected Logger logger = Logger.getLogger(this.getClass().getName());

    /**
     * Instantiates a new Actor.
     *
     * @param runtimeContext Context for the runtime.
     * @param id             Actor identifier.
     */
    protected StrajDBActor(ActorRuntimeContext runtimeContext, ActorId id) {
        super(runtimeContext, id);
    }
}
