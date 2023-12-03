package cn.edu.suda.ada.ect;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class PushApplication {
    // compute node本身根据id存储了状态，可以被其他的不相干查询服用
    // 比如查询A需要T1附近的10条轨迹，里面计算了T1和T2的距离；查询B需要T2附近的10条轨迹，这时候就能服用了
    public static void main(String[] args) {
        //Spring应用启动起来
        SpringApplication.run(PushApplication.class, args);
    }
}