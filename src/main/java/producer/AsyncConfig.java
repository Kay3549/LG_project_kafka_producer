package producer;


import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Configuration
public class AsyncConfig {
	
	@Bean(name = "taskExecutor")
    public Executor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(100);  // 코어 쓰레드 수
        executor.setMaxPoolSize(200);   // 최대 쓰레드 수
        executor.setQueueCapacity(5000);  // 큐 용량
        executor.setThreadNamePrefix("KafkaProducer-");
        executor.initialize();
        return executor;
    }

    @Bean
    public ExecutorService executorService() {
        return Executors.newFixedThreadPool(100); // 고정된 크기의 쓰레드 풀을 생성
    }

}
