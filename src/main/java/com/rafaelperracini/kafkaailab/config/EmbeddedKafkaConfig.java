package com.rafaelperracini.kafkaailab.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.EmbeddedKafkaZKBroker;

/**
 * Kafka embutido — sobe um broker Kafka em memoria junto com a aplicacao.
 *
 * NAO depende de Docker, Zookeeper externo ou instalacao manual do Kafka.
 * O spring-kafka-test fornece o EmbeddedKafkaBroker que inicia tudo programaticamente.
 *
 * Em producao, trocar por um Kafka real (remover esta classe e apontar
 * spring.kafka.bootstrap-servers para o cluster real).
 *
 * Topicos criados automaticamente:
 * - "pedidos" — recebe pedidos novos (Producer publica aqui)
 * - "pedidos-classificados" — recebe pedidos com risco classificado pela IA
 */
@Configuration
public class EmbeddedKafkaConfig {

    private static final Logger log = LoggerFactory.getLogger(EmbeddedKafkaConfig.class);

    @Bean(initMethod = "afterPropertiesSet", destroyMethod = "destroy")
    public EmbeddedKafkaBroker embeddedKafkaBroker() {
        log.info("Iniciando Kafka embutido na porta 9092...");
        EmbeddedKafkaBroker broker = new EmbeddedKafkaZKBroker(1, true, "pedidos", "pedidos-classificados")
                .kafkaPorts(9092)
                .brokerListProperty("spring.kafka.bootstrap-servers");
        log.info("Kafka embutido configurado com topicos: pedidos, pedidos-classificados");
        return broker;
    }
}
