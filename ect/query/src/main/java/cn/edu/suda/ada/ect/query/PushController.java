package cn.edu.suda.ada.ect.query;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class PushController {
    @GetMapping("/test")
    String test(){
        return "hello";
    }
}
