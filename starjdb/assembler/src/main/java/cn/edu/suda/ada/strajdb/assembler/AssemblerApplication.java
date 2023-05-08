package cn.edu.suda.ada.strajdb.assembler;

import cn.edu.suda.ada.strajdb.serializers.KryoSerializer;
import io.dapr.actors.runtime.ActorRuntime;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.time.Duration;

@SpringBootApplication
public class AssemblerApplication {

    public static void main(String[] args) {
        // Idle timeout until actor instance is deactivated.
        ActorRuntime.getInstance().getConfig().setActorIdleTimeout(Duration.ofSeconds(10));
        // How often actor instances are scanned for deactivation and balance.
        ActorRuntime.getInstance().getConfig().setActorScanInterval(Duration.ofSeconds(30));
        // How long to wait until for draining an ongoing API call for an actor instance.
        ActorRuntime.getInstance().getConfig().setDrainOngoingCallTimeout(Duration.ofSeconds(10));
        // Determines whether to drain API calls for actors instances being balanced.
        ActorRuntime.getInstance().getConfig().setDrainBalancedActors(true);

        // Register the Actor class.
        ActorRuntime.getInstance().registerActor(AssemblerActorImpl.class, new KryoSerializer(), new KryoSerializer());

        SpringApplication.run(AssemblerApplication.class, args);
    }

}
