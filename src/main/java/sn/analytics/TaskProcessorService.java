package sn.analytics;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Boot up all components
 */
@SpringBootApplication
@ComponentScan({"sn.analytics"})
@EnableScheduling
public class TaskProcessorService {


    public static void main(String [] args){
        SpringApplication.run(TaskProcessorService.class,args);
    }
}
