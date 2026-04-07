package com.rafaelperracini.kafkaailab.gateway.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rafaelperracini.kafkaailab.dto.Pedido;
import com.rafaelperracini.kafkaailab.dto.PedidoClassificado;
import com.rafaelperracini.kafkaailab.gateway.PedidoKafkaGateway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class PedidoKafkaGatewayImpl implements PedidoKafkaGateway {

    private static final Logger log = LoggerFactory.getLogger(PedidoKafkaGatewayImpl.class);
    private static final String TOPICO_PEDIDOS = "pedidos";
    private static final String TOPICO_CLASSIFICADOS = "pedidos-classificados";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public PedidoKafkaGatewayImpl(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @Override
    public void publicarPedido(Pedido pedido) {
        try {
            String json = objectMapper.writeValueAsString(pedido);
            kafkaTemplate.send(TOPICO_PEDIDOS, pedido.id(), json);
            log.info("Pedido publicado no topico '{}': {}", TOPICO_PEDIDOS, pedido.id());
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Erro ao serializar pedido: " + e.getMessage(), e);
        }
    }

    @Override
    public void publicarClassificacao(String key, PedidoClassificado classificado) {
        try {
            String json = objectMapper.writeValueAsString(classificado);
            kafkaTemplate.send(TOPICO_CLASSIFICADOS, key, json);
            log.info("Resultado publicado no topico '{}'", TOPICO_CLASSIFICADOS);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Erro ao serializar classificacao: " + e.getMessage(), e);
        }
    }
}
