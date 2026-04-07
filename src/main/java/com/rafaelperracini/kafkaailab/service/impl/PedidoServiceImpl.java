package com.rafaelperracini.kafkaailab.service.impl;

import com.rafaelperracini.kafkaailab.dto.Pedido;
import com.rafaelperracini.kafkaailab.dto.PedidoClassificado;
import com.rafaelperracini.kafkaailab.kafka.PedidoConsumer;
import com.rafaelperracini.kafkaailab.kafka.PedidoProducer;
import com.rafaelperracini.kafkaailab.service.PedidoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
public class PedidoServiceImpl implements PedidoService {

    private static final Logger log = LoggerFactory.getLogger(PedidoServiceImpl.class);

    private final PedidoProducer producer;
    private final PedidoConsumer consumer;

    public PedidoServiceImpl(PedidoProducer producer, PedidoConsumer consumer) {
        this.producer = producer;
        this.consumer = consumer;
    }

    @Override
    public Map<String, String> criar(Pedido pedido) {
        String id = pedido.id() != null ? pedido.id() : UUID.randomUUID().toString();
        Pedido pedidoComId = new Pedido(id, pedido.cliente(), pedido.valor(),
                pedido.descricao(), pedido.quantidadeItens());

        try {
            producer.enviar(pedidoComId);
        } catch (Exception e) {
            log.error("Erro ao enviar pedido ao Kafka: {}", e.getMessage(), e);
            return Map.of("status", "Erro ao enviar pedido", "erro", e.getMessage());
        }

        return Map.of(
                "status", "Pedido enviado ao Kafka",
                "id", pedidoComId.id(),
                "info", "Consulte GET /pedidos/classificados em alguns segundos"
        );
    }

    @Override
    public List<PedidoClassificado> listarClassificados() {
        return consumer.getResultados();
    }
}
