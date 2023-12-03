package cn.edu.suda.ada.ect.types;

import io.dapr.actors.ActorMethod;
import io.dapr.actors.ActorType;
import reactor.core.publisher.Mono;

@ActorType(name = "assembler")
public interface AssemblerActor {
    @ActorMethod(returns = Void.class, name = "acceptNewPoint")
    Mono<Void> acceptNewPoint(AssemblerMsg msg);
}
