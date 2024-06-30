package producer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;


@SpringBootApplication
@EnableAsync
public class MainRunningApp {

	
	public static void main(String[] args) {
		
		SpringApplication.run(MainRunningApp.class, args);

	}
	
}
