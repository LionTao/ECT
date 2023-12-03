package cn.edu.suda.ada.ect.query;

import com.bedatadriven.jackson.datatype.jts.JtsModule;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@Configuration
@SpringBootApplication
//@EnableJpaRepositories(basePackages = "cn.edu.suda.ada.ect.query.repo")
//@EntityScan(basePackageClasses = {City.class})
@ComponentScan(value = {"cn.*"})
public class QueryAgentApplication {
    // compute node本身根据id存储了状态，可以被其他的不相干查询服用
    // 比如查询A需要T1附近的10条轨迹，里面计算了T1和T2的距离；查询B需要T2附近的10条轨迹，这时候就能服用了
    public static void main(String[] args) {
        //Spring应用启动起来
        SpringApplication.run(QueryAgentApplication.class, args);
//        GreetingClient greetingClient = context.getBean(GreetingClient.class);
//        // We need to block for the content here or the JVM might exit before the message is logged
//        System.out.println(">> message = " + greetingClient.getMessage().block());
    }

    @Bean
    public JtsModule jtsModule() {
        // This module will provide a Serializer for geometries
        return new JtsModule();
    }
}