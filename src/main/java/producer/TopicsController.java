package producer;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

@Controller
@Slf4j
public class TopicsController {

	@Autowired
	private KafkaProducerApp Producer;

	@PostMapping("/gcapi/post/{topic}")
	public Mono<ResponseEntity<String>> GetApiData(@PathVariable("topic") String tranId, @RequestBody(required=false) String msg) {

		String topic_name = tranId;
		log.info("토픽이름 : {}", topic_name);
		log.info("프로듀서가 받음. 메시지 : {}", msg);

		try {
			
			List<CompletableFuture<Void>> futures = new ArrayList<>();
			CompletableFuture<Void> future = Producer.sendMessageAsync(topic_name, "", msg);
			futures.add(future);
			CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
			
			if(topic_name.equals("from_clcc_mblaiccmpnrs_message")) {
				return Mono.just(ResponseEntity.ok().body(String.format("Producer has got an RT message for '%s' : %s",topic_name, msg)));
			}else if(topic_name.equals("from_clcc_hmaiccmpnrs_message")) {
				return Mono.just(ResponseEntity.ok().body(String.format("Producer has got an RT message for '%s' : %s",topic_name, msg)));
			}else if(topic_name.equals("from_clcc_mblucrmcmpnrs_message")) {
				return Mono.just(ResponseEntity.ok().body(String.format("Producer has got an RT message for '%s' : %s",topic_name, msg)));
			}else if(topic_name.equals("from_clcc_hmucrmcmpnrs_message")) {
				return Mono.just(ResponseEntity.ok().body(String.format("Producer has got an RT message for '%s' : %s",topic_name, msg)));
			}else if(topic_name.equals("from_clcc_hmucrmcmpnma_message")) {
				return Mono.just(ResponseEntity.ok().body(String.format("Producer has got an MA message for '%s' : %s",topic_name, msg)));
			}else if(topic_name.equals("from_clcc_mblucrmcmpnma_message")) {
				return Mono.just(ResponseEntity.ok().body(String.format("Producer has got an MA message for '%s' : %s",topic_name, msg)));
			}else if(topic_name.equals("from_clcc_hmaiccmpnma_message")) {
				return Mono.just(ResponseEntity.ok().body(String.format("Producer has got an MA message for '%s' : %s",topic_name, msg)));
			}else if(topic_name.equals("from_clcc_mblaiccmpnma_message")) {
				return Mono.just(ResponseEntity.ok().body(String.format("Producer has got an MA message for '%s' : %s",topic_name, msg)));
			}
			
			
		} catch (Exception e) {
			log.info("Error, Producer side : {}", e.getMessage()  );
			e.printStackTrace();
		}
		
		return Mono.just(ResponseEntity.ok("GetApiData Method has been called by G.C Application well"));
	}

	@PostMapping("/360view/{topic}")
	public Mono<ResponseEntity<String>> Get360viewData(@PathVariable("topic") String tranId, @RequestBody(required=false) String msg) {

		String topic_name = tranId;
		log.info("토픽이름 : {}", topic_name);
		log.info("프로듀서가 받음. 메시지 : {}", msg);
		
		try {
			
			List<CompletableFuture<Void>> futures = new ArrayList<>();
			CompletableFuture<Void> future = Producer.sendMessageAsync(topic_name, "", msg);
			futures.add(future);
			CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
			
			return Mono.just(ResponseEntity.ok().body(String.format("Producer has got an message for '%s' : %s",topic_name, msg)));
			
		} catch (Exception e) {
			log.info("Error, Producer side : {}", e.getMessage()  );
			e.printStackTrace();
		}

		return Mono.just(ResponseEntity.ok("Get360viewData Method has been called by G.C Application well"));
	}

	@GetMapping("/gethc")
	public Mono<ResponseEntity<String>> gealthCheck() throws Exception {
		return Mono.just(ResponseEntity.ok("TEST RESPONSE"));
	}
	
	
	@GetMapping("/apim-gw")
	public Mono<ResponseEntity<String>> getHealthCheckAPIM() throws Exception {
		return Mono.just(ResponseEntity.ok("TEST RESPONSE"));
	}
	
	@GetMapping("/kafka-gw")
	public Mono<ResponseEntity<String>> getHealthCheckKafka() throws Exception {
		return Mono.just(ResponseEntity.ok("TEST RESPONSE"));
	}
	
	/**
	 * [EKS] POD LivenessProbe 헬스체크
	 */
	private final Instant started = Instant.now();
	
    @GetMapping("/healthz")
    public ResponseEntity<String> healthCheck() {
        Duration duration = Duration.between(started, Instant.now());
        if (duration.getSeconds() > 10) {
            return ResponseEntity.status(500).body("error: " + duration.getSeconds());
        } else {
            return ResponseEntity.ok("ok");
        }
    }

}