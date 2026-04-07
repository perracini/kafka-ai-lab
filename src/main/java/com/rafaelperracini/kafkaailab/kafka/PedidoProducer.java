package com.rafaelperracini.kafkaailab.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rafaelperracini.kafkaailab.dto.Pedido;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Publica pedidos no topico "pedidos" do Kafka.
 * O Consumer vai consumir e classificar com IA.
 */
@Component
public class PedidoProducer {

    private static final Logger log = LoggerFactory.getLogger(PedidoProducer.class);
    private static final String TOPICO = "pedidos";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public PedidoProducer(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public void enviar(Pedido pedido) throws JsonProcessingException {
        String json = objectMapper.writeValueAsString(pedido);
        kafkaTemplate.send(TOPICO, pedido.id(), json);
        log.info("Pedido publicado no topico '{}': {}", TOPICO, pedido.id());
    }
}
