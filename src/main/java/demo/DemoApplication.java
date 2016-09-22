package demo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.kafka.Kafka09;
import org.springframework.integration.dsl.kafka.Kafka09MessageDrivenChannelAdapterSpec.KafkaMessageDrivenChannelAdapterListenerContainerSpec;
import org.springframework.integration.dsl.kafka.Kafka09ProducerMessageHandlerSpec;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@EnableIntegration
@SpringBootApplication
public class DemoApplication {

    public static final String TEST_TOPIC_ID = "event-stream";

    /**
     * common values used in both the consumer and producer configuration classes.
     * This is a poor-man's {@link org.springframework.boot.context.properties.ConfigurationProperties}!
     */
    @Component
    public static class KafkaConfig {

        @Value("${kafka.topic:" + TEST_TOPIC_ID + "}")
        private String topic;

        @Value("${kafka.address:localhost:9092}")
        private String brokerAddress;

        KafkaConfig() {
        }

        public KafkaConfig(String t, String b, String zk) {
            this.topic = t;
            this.brokerAddress = b;
        }

        public String getTopic() {
            return topic;
        }

        public String getBrokerAddress() {
            return brokerAddress;
        }
    }

    @Configuration
    public static class ProducerConfiguration {

        @Autowired
        private KafkaConfig kafkaConfig;

        private static final String OUTBOUND_ID = "outbound";

        private Log log = LogFactory.getLog(getClass());

        @Bean
        public ProducerFactory<String, String> producerFactory() {
            Map<String, Object> props = new HashMap<>();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaConfig.getBrokerAddress());
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            return new DefaultKafkaProducerFactory<>(props);
        }

        @Bean
        @DependsOn(OUTBOUND_ID)
        CommandLineRunner kickOff(@Qualifier(OUTBOUND_ID + ".input") MessageChannel in) {
            return args -> {
                for (int i = 0; i < 1000; i++) {
                    //in.send(new GenericMessage<>("#" + i));
                    in.send(MessageBuilder.withPayload("#" + i).setHeader(KafkaHeaders.TOPIC, this.kafkaConfig.getTopic()).build());
                    log.info("sending message #" + i);
                }
            };
        }


        @Bean(name = OUTBOUND_ID)
        IntegrationFlow producer() {
            log.info("starting producer flow..");

            return flowDefinition -> {
                Kafka09ProducerMessageHandlerSpec kafka09ProducerMessageHandlerSpec = Kafka09.outboundChannelAdapter(producerFactory())
                        .topic(this.kafkaConfig.getTopic())
                        .messageKey(m -> m.getHeaders().get(IntegrationMessageHeaderAccessor.SEQUENCE_NUMBER));

                flowDefinition
                        .handle(kafka09ProducerMessageHandlerSpec);
            };
        }
    }

    @Configuration
    public static class ConsumerConfiguration {

        @Autowired
        private KafkaConfig kafkaConfig;

        private Log log = LogFactory.getLog(getClass());

        @Bean
        public ConsumerFactory<String, String> consumerFactory() {
            Map<String, Object> props = new HashMap<>();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getBrokerAddress());
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "myGroup");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
            return new DefaultKafkaConsumerFactory<>(props);
        }

        @Bean
        IntegrationFlow consumer() {
            log.info("starting consumer..");

            KafkaMessageDrivenChannelAdapterListenerContainerSpec<String, String> kafkaMDCAListenerContainerSpec =
                    Kafka09.messageDriverChannelAdapter(consumerFactory(), kafkaConfig.getTopic());

            return IntegrationFlows
                    .from(kafkaMDCAListenerContainerSpec)
                    .handle((payload, headers) -> {
                        log.info(payload);
                        return null;
                    })
                    .get();
        }
    }

    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }
}
