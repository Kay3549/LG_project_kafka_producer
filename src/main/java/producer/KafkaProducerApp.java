package producer;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

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
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;

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
	private  String sasl;
	@Value("${producer.protocal}")
	private  String protocal;
	@Value("${producer.mechanism}")
	private  String mechanism;

	@PostConstruct
	public void initialize() {// 카프카 프로듀서 서버 초기화

		PRODUCER_IP = ip;
		PRODUCER_SASL = sasl;
		PRODUCER_PROTOCAL = protocal;
		PRODUCER_MECHANISM = mechanism;

		String saslJassConfig = PRODUCER_SASL;
		
		log.info("IP Address : {}",PRODUCER_IP);
		log.info("authentication info : {}",saslJassConfig);
		log.info("protocal : {}",PRODUCER_PROTOCAL);
		log.info("mechanism : {}",PRODUCER_MECHANISM);
		
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
		
		log.info("프롭 : {}",   props.toString() );

	}

	@Async
	public CompletableFuture<Void> sendMessageAsync(String topic, String key, String message) {// sendMessage함수를 비동기로 실행
		CompletableFuture<Void> future = new CompletableFuture<>();
		
		log.info("ClassName : KafkaProducerApp & Method : sendMessageAsync");
		log.info("====== sendMessageAsync ======");
		log.info("Message from GcApp : {}",message);
		log.info("Topic Name : {}",topic);
		log.info("Message Key : {}",key);
		

		try {
			sendMessage(topic, key, message);
			future.complete(null);
		} catch (Exception e) {
			future.completeExceptionally(e);
		}

		log.info("====== End sendMessageAsync ======");
		return future;
	}

	public void sendMessage(String topic, String key, String message) {
		
		log.info(" ");
		log.info("====== ClassName : KafkaProducerApp & Method : sendMessage ======");
		Producer<String, String> producer = new KafkaProducer<String, String>(props);
		ObjectMapper mapper = new ObjectMapper();
		Map<String,Object> map = new HashMap<String, Object>();

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
			log.info("value : {}", value);

			String payload = mapper.writeValueAsString(value);
			log.info("payload : {}", payload);
			
			JSONObject msg = new JSONObject();
			msg.put("headers",headers);
			msg.put("payload",value);
					
			log.info("msg.toString : {}", msg.toString());
			ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, msg.toString());

			producer.send(record, new Callback() {

				@Override
				public void onCompletion(RecordMetadata metadata, Exception e) {

					SimpleDateFormat form = new SimpleDateFormat("MM/dd == hh:mm:ss");
					Date now = new Date();
					String nowtime = form.format(now);

					if (metadata != null) {

						String infoString = String.format("Success partition : %d, offset : %d", metadata.partition(),
								metadata.offset());
						log.info("Sent message to kafka server successfully : {}", nowtime);
						log.info("Information from the kafka server : {}",infoString);

					} else {

						String infoString = String.format("Failed %s", e.getMessage());
						log.error(infoString);

						int maxRetries = 3;
						int retryCount = 0;
						while (metadata == null && retryCount < maxRetries) {
							log.info("Retrying...");
							producer.send(record, this); // 메시지 재전송.
							retryCount++;
						}

						if (metadata == null) {
							log.error("Max retries reached. Unable to send the message.");
						}
					}

				}
			});

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			log.info("====== End sendMessage ======");
			producer.close();
		}

	}

}
