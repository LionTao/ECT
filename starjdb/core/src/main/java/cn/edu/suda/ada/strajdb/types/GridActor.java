package cn.edu.suda.ada.strajdb.types;

import io.dapr.actors.ActorMethod;
import io.dapr.actors.ActorType;
import reactor.core.publisher.Mono;

import java.util.List;

@ActorType(name = "IndexActor")
public interface GridActor {
    @ActorMethod(returns = Boolean.class, name = "acceptNewGeometry")
    Mono<Boolean> acceptNewGeometry(GridMsg g);

    @ActorMethod(returns = List.class, name = "query")
    Mono<List<String>> query(String data);
}
