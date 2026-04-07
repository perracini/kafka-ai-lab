package com.rafaelperracini.kafkaailab.service.impl;

import com.rafaelperracini.kafkaailab.dto.Pedido;
import com.rafaelperracini.kafkaailab.dto.PedidoClassificado;
import com.rafaelperracini.kafkaailab.gateway.PedidoKafkaGateway;
import com.rafaelperracini.kafkaailab.repository.PedidoClassificadoRepository;
import com.rafaelperracini.kafkaailab.service.ClassificadorRiscoService;
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

    private final PedidoKafkaGateway kafkaGateway;
    private final ClassificadorRiscoService classificadorService;
    private final PedidoClassificadoRepository repository;

    public PedidoServiceImpl(PedidoKafkaGateway kafkaGateway,
                             ClassificadorRiscoService classificadorService,
                             PedidoClassificadoRepository repository) {
        this.kafkaGateway = kafkaGateway;
        this.classificadorService = classificadorService;
        this.repository = repository;
    }

    @Override
    public Map<String, String> criar(Pedido pedido) {
        String id = pedido.id() != null ? pedido.id() : UUID.randomUUID().toString();
        Pedido pedidoComId = new Pedido(id, pedido.cliente(), pedido.valor(),
                pedido.descricao(), pedido.quantidadeItens());

        try {
            kafkaGateway.publicarPedido(pedidoComId);
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
        return repository.listarTodos();
    }

    @Override
    public void processar(Pedido pedido) {
        log.info("Pedido recebido: {} — classificando risco com IA...", pedido.id());

        PedidoClassificado classificado = classificadorService.classificar(pedido);
        log.info("Pedido {} classificado: RISCO={}", pedido.id(), classificado.risco());

        kafkaGateway.publicarClassificacao(pedido.id(), classificado);
        repository.salvar(classificado);
    }
}
