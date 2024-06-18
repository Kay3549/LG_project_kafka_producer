package producer;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

@Service
@Slf4j
public class KafkaProducerApp {

	private Properties props = new Properties();
	private static String PRODUCER_IP;
	private static String PRODUCER_SASL;
	private static String PRODUCER_PROTOCAL;
	private static String PRODUCER_MECHANISM;

	@Value("${producer.ip}")
	public String ip;
	@Value("${producer.sasl}")
	private String sasl;
	@Value("${producer.protocal}")
	private String protocal;
	@Value("${producer.mechanism}")
	private String mechanism;

	@PostConstruct
	public void initialize() {// 카프카 프로듀서 서버 초기화

		PRODUCER_IP = ip;
		PRODUCER_SASL = sasl;
		PRODUCER_PROTOCAL = protocal;
		PRODUCER_MECHANISM = mechanism;

		String saslJassConfig = PRODUCER_SASL;

		log.info("IP 주소 : {}", PRODUCER_IP);
		log.info("authentication 인증정보 : {}", saslJassConfig);
		log.info("프로토콜 : {}", PRODUCER_PROTOCAL);
		log.info("메커니즘 : {}", PRODUCER_MECHANISM);

		// SASL configuration part
//		String saslJassConfig = "org.apache.kafka.common.security.scram.ScramLoginModule required" + " username="
//				+ "clcc_cc_svc" // SASL ID
//				+ " password=" + "GPesEI6k78DEku58" // SASL PASSWORD(개발)
////	            + " password=" + "mYmkZ147fSM9CB3e"  // SASL PASSWORD(운영)
//				+ ";";

		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, PRODUCER_IP); // 서버,포트 설정. (실제로 서버와 포트 번호로 변경될 부분)
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// sasl 설정 파트
		props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, PRODUCER_PROTOCAL);
		props.put(SaslConfigs.SASL_MECHANISM, PRODUCER_MECHANISM);
		props.put(SaslConfigs.SASL_JAAS_CONFIG, saslJassConfig);

		log.info("프롭 : {}", props.toString());

	}

	public Mono<RecordMetadata> sendMessage(String topic, String key, String message) {

		return Mono.create(sink -> {
			log.info(" ");
			log.info("====== ClassName : KafkaProducerApp & Method : sendMessage ======");
			log.info("GcApp로 부터의 메시지 : {}", message);
			log.info("토픽명 : {}", topic);
			log.info("메시지 키 : {}", key);
			Producer<String, String> producer = new KafkaProducer<>(props);
			ObjectMapper mapper = new ObjectMapper();
			Map<String, Object> map = new HashMap<>();

			String value = "";

			try {

				LocalDateTime now = LocalDateTime.now();
				DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
				String topcDataIsueDtm = now.format(formatter);

				String mgid = UUID.randomUUID().toString();
				String gtid = UUID.randomUUID().toString();
				map.put("ID", mgid);
				map.put("DESTINATION", topic);
				map.put("DATE", topcDataIsueDtm);
				map.put("X-App-Name", "clcc_cc_svc");
				map.put("X-Global-Transaction-ID", gtid);
				JSONObject headers = new JSONObject(map);

				value = message;
				log.info("값 : {}", value);

				String payload = mapper.writeValueAsString(value);
				log.info("페이로드 : {}", payload);

				JSONObject msg = new JSONObject();
				msg.put("headers", headers);
				msg.put("payload", value);

				log.info("String으로 변환한 전체 메시지 : {}", msg.toString());
				ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, msg.toString());

				sendWithRetries(producer, record, 3)
				.doOnSuccess(metadata -> sink.success(metadata))
				.doOnError(sink::error)
				.onErrorResume(e -> {
					log.error("Error 발생!!: {}", e.getMessage());
					return Mono.empty();
				}).subscribe();

			} catch (Exception e) {
				sink.error(e);
			} finally {
				log.info("====== End sendMessage ======");
				producer.close();
			}
		});
	}
	

	private Mono<RecordMetadata> sendWithRetries(Producer<String, String> producer,
			ProducerRecord<String, String> record, int maxRetries) {
		return Mono.create(sink -> {
			AtomicBoolean success = new AtomicBoolean(false);
			int retryCount = 0;

			while (retryCount < maxRetries && !success.get()) {

				producer.send(record, new Callback() {

					@Override
					public void onCompletion(RecordMetadata metadata, Exception e) {

						SimpleDateFormat form = new SimpleDateFormat("MM/dd == hh:mm:ss");
						Date now = new Date();
						String nowtime = form.format(now);

						if (metadata != null && metadata.partition() != -1 && metadata.offset() != -1) {
							String infoString = String.format("Success partition : %d, offset : %d",
									metadata.partition(), metadata.offset());
							log.info("카프카 서버로 메시지를 성공적으로 보냈습니다.토픽({}) : {}", nowtime, record.topic());
							log.info("카프카 서버로 부터 받은 토픽 정보 : {}", infoString);
							success.set(true);
							sink.success(metadata);
						} else {

							String errorMessage = "카프카 브로커 서버로 메시지를 보내는 것에 실패하였습니다. => 에러 메시지";
							if (e != null) {
								errorMessage += " : " + e.getMessage();
							} else {
								log.error("partition과 offset 값으로 '-1'을 리턴 받는 것은 메시지 전송 실패를 의미합니다.");
							}

							log.error(errorMessage);
							success.set(false);
						}
					}
				});

				if (!success.get()) {
					log.info("재시도... 횟수 {}", retryCount + 1);
					retryCount++;
				}
			}

			if (!success.get()) {
				log.error("재시도 최고 횟수에 도달하였습니다. 메시지를 보낼 수 없습니다.");
				sink.error(new RuntimeException("재시도 최고 횟수에 도달하였습니다. 메시지를 보낼 수 없습니다."));
			}
		});
	}

	
	
}
