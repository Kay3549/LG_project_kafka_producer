package producer;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Scheduled;
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
	public Mono<ResponseEntity<String>> GetApiData(@PathVariable("topic") String topic,
			@RequestBody(required = false) String msg) {

		log.info("토픽으로 보낼 메시지를 받음. 토픽명 : {}", topic);
		log.info("토픽 / 메시지 내용 : {} // {}", topic, msg);

		return Producer.sendMessage(topic, "", msg).flatMap(metadata -> {
			String messageType = topic.startsWith("from_clcc_") ? (topic.contains("rs") ? "RT" : "MA") : "";
			String responseMessage = String.format("'%s' 토픽으로 %s message를 보냄. 메시지 내용 : %s",
					messageType.isEmpty() ? "regular" : topic, messageType, msg);
			return Mono.just(ResponseEntity.ok().body(responseMessage));
		}).doOnError(e -> {
			log.error("카프카 서버로 메시지를 보내는 도중 에러가 발생하였습니다. : {}", e.getMessage());
		}).onErrorResume(e -> {
			String errorMessage = String.format("카프카 서버와 통신 중 에러가 발생하였습니다. : %s", e.getMessage());
			return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorMessage));
		});
	}

	@PostMapping("/360view/{topic}")
	public Mono<ResponseEntity<String>> Get360viewData(@PathVariable("topic") String tranId,
			@RequestBody(required = false) String msg) {

		String topic_name = tranId;
		log.info("토픽이름 : {}", topic_name);
		log.info("프로듀서가 받음. 메시지 : {}", msg);

		try {
			return Producer.sendMessage(topic_name, "", msg).flatMap(metadata -> {
				String responseMessage = String.format("토픽 '%s'로 보낼 360view 메시지를 G.C Application으로부터 받았습니다. 메시지 : %s",
						topic_name, msg);
				return Mono.just(ResponseEntity.ok().body(responseMessage));
			}).doOnError(e -> {
				log.error("카프카 서버로 메시지를 보내는 도중 에러가 발생하였습니다. : {}", e.getMessage());
			}).onErrorResume(e -> {
				String errorMessage = String.format("카프카 서버와 통신 중 에러가 발생하였습니다. : %s", e.getMessage());
				return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorMessage));
			});
		} catch (Exception e) {
			log.error("프로듀서쪽의 에러 : {}", e.getMessage());
			return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("비동기 진행과정 중에서 에러가 발생했습니다."));
		}

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
	
	
	@Scheduled(cron = "0 3 0 * * *") // 매일 12:03에 실행
	public void startlogs() {
		
		Date today = new Date();

		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy년 MM월 dd일 HH시 mm분 ss초");
		log.info("{}, producer 로그 시작",dateFormat.format(today).toString());
	
	}

}