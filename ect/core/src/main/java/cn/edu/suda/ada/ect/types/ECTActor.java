package cn.edu.suda.ada.ect.types;

import io.dapr.actors.ActorId;
import io.dapr.actors.runtime.AbstractActor;
import io.dapr.actors.runtime.ActorRuntimeContext;

import java.util.logging.Logger;

public abstract class ECTActor extends AbstractActor {
    protected Logger logger = Logger.getLogger(this.getClass().getName());

    /**
     * Instantiates a new Actor.
     *
     * @param runtimeContext Context for the runtime.
     * @param id             Actor identifier.
     */
    protected ECTActor(ActorRuntimeContext runtimeContext, ActorId id) {
        super(runtimeContext, id);
    }
}
