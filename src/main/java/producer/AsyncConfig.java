package producer;


import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
public class AsyncConfig {
	
	@Bean(name = "WH_taskExecutor")
    public ThreadPoolTaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(10000);  // Core thread count
        executor.setMaxPoolSize(20000);   // Max thread count
        executor.setQueueCapacity(500000);  // Queue capacity
        executor.setThreadNamePrefix("WH_Custom_Kafkaprducer-");
        executor.setAwaitTerminationSeconds(60);  // Wait time for tasks to complete before shutdown
        executor.initialize();
        return executor;
    }

}
