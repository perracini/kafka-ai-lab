package com.rafaelperracini.kafkaailab.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rafaelperracini.kafkaailab.dto.Pedido;
import com.rafaelperracini.kafkaailab.service.PedidoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PedidoConsumer {

    private static final Logger log = LoggerFactory.getLogger(PedidoConsumer.class);

    private final PedidoService pedidoService;
    private final ObjectMapper objectMapper;

    public PedidoConsumer(PedidoService pedidoService, ObjectMapper objectMapper) {
        this.pedidoService = pedidoService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "pedidos", groupId = "kafka-ai-lab")
    public void consumir(String mensagem) {
        try {
            Pedido pedido = objectMapper.readValue(mensagem, Pedido.class);
            pedidoService.processar(pedido);
        } catch (Exception e) {
            log.error("Erro ao processar pedido: {}", e.getMessage(), e);
        }
    }
}
