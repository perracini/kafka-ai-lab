package com.rafaelperracini.kafkaailab.controller;

import com.rafaelperracini.kafkaailab.dto.Pedido;
import com.rafaelperracini.kafkaailab.dto.PedidoClassificado;
import com.rafaelperracini.kafkaailab.kafka.PedidoConsumer;
import com.rafaelperracini.kafkaailab.kafka.PedidoProducer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/pedidos")
public class PedidoController {

    private final PedidoProducer producer;
    private final PedidoConsumer consumer;

    public PedidoController(PedidoProducer producer, PedidoConsumer consumer) {
        this.producer = producer;
        this.consumer = consumer;
    }

    @PostMapping
    public Map<String, String> criarPedido(@RequestBody Pedido pedido) throws Exception {
        Pedido pedidoComId = new Pedido(
            pedido.id() != null ? pedido.id() : UUID.randomUUID().toString(),
            pedido.cliente(),
            pedido.valor(),
            pedido.descricao(),
            pedido.quantidadeItens()
        );

        producer.enviar(pedidoComId);

        return Map.of(
            "status", "Pedido enviado ao Kafka",
            "id", pedidoComId.id(),
            "info", "Consulte GET /pedidos/classificados em alguns segundos"
        );
    }

    @GetMapping("/classificados")
    public List<PedidoClassificado> listarClassificados() {
        return consumer.getResultados();
    }
}
