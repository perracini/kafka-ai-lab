package com.rafaelperracini.kafkaailab.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rafaelperracini.kafkaailab.dto.Pedido;
import com.rafaelperracini.kafkaailab.dto.PedidoClassificado;
import com.rafaelperracini.kafkaailab.service.ClassificadorRiscoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Consumer Kafka que integra com IA:
 *
 * 1. Escuta o topico "pedidos"
 * 2. Deserializa o JSON em Pedido
 * 3. Chama o ClassificadorRiscoService (IA via Ollama)
 * 4. Publica o resultado no topico "pedidos-classificados"
 * 5. Armazena em memoria para consulta via GET /pedidos/classificados
 */
@Component
public class PedidoConsumer {

    private static final Logger log = LoggerFactory.getLogger(PedidoConsumer.class);
    private static final String TOPICO_CLASSIFICADOS = "pedidos-classificados";

    private final ClassificadorRiscoService classificadorService;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final List<PedidoClassificado> resultados = new CopyOnWriteArrayList<>();

    public PedidoConsumer(ClassificadorRiscoService classificadorService,
                          KafkaTemplate<String, String> kafkaTemplate,
                          ObjectMapper objectMapper) {
        this.classificadorService = classificadorService;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "pedidos", groupId = "kafka-ai-lab")
    public void consumir(String mensagem) {
        try {
            Pedido pedido = objectMapper.readValue(mensagem, Pedido.class);
            log.info("Pedido recebido: {} — classificando risco com IA...", pedido.id());

            PedidoClassificado classificado = classificadorService.classificar(pedido);
            log.info("Pedido {} classificado: RISCO={}", pedido.id(), classificado.risco());

            String json = objectMapper.writeValueAsString(classificado);
            kafkaTemplate.send(TOPICO_CLASSIFICADOS, pedido.id(), json);
            log.info("Resultado publicado no topico '{}'", TOPICO_CLASSIFICADOS);

            resultados.add(classificado);
        } catch (Exception e) {
            log.error("Erro ao processar pedido: {}", e.getMessage(), e);
        }
    }

    public List<PedidoClassificado> getResultados() {
        return List.copyOf(resultados);
    }
}
