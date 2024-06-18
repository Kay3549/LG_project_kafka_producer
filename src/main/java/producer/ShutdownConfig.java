package producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

@Configuration

public class ShutdownConfig {

	@Autowired
	private ExecutorService taskExecutor;

	@EventListener
	public void onContextClosed(ContextClosedEvent event) {
		taskExecutor.shutdown();
		try {
			if (!taskExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
				taskExecutor.shutdownNow();
			}
		} catch (InterruptedException e) {
			taskExecutor.shutdownNow();
		}
	}
}
