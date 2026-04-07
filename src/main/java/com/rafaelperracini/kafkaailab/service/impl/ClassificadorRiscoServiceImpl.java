package com.rafaelperracini.kafkaailab.service.impl;

import com.rafaelperracini.kafkaailab.dto.Pedido;
import com.rafaelperracini.kafkaailab.dto.PedidoClassificado;
import com.rafaelperracini.kafkaailab.service.ClassificadorRiscoService;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.stereotype.Service;

/**
 * Classifica o risco de fraude de um pedido usando IA (Llama 3.2 via Ollama).
 *
 * O prompt envia os dados do pedido e pede classificacao em ALTO, MEDIO ou BAIXO.
 * A IA responde com a classificacao e uma justificativa curta.
 */
@Service
public class ClassificadorRiscoServiceImpl implements ClassificadorRiscoService {

    private final ChatClient chatClient;

    public ClassificadorRiscoServiceImpl(ChatClient.Builder chatClientBuilder) {
        this.chatClient = chatClientBuilder
                .defaultSystem(
                    "Voce e um analista de risco de fraude em e-commerce. " +
                    "Classifique pedidos como ALTO, MEDIO ou BAIXO risco. " +
                    "Responda SEMPRE neste formato exato:\n" +
                    "RISCO: [ALTO|MEDIO|BAIXO]\n" +
                    "JUSTIFICATIVA: [uma frase curta]\n" +
                    "Nao adicione nada alem dessas duas linhas."
                )
                .build();
    }

    @Override
    public PedidoClassificado classificar(Pedido pedido) {
        String prompt = String.format(
            "Classifique o risco de fraude deste pedido:\n" +
            "- Cliente: %s\n" +
            "- Valor: R$ %.2f\n" +
            "- Descricao: %s\n" +
            "- Quantidade de itens: %d",
            pedido.cliente(), pedido.valor(), pedido.descricao(), pedido.quantidadeItens()
        );

        String resposta = chatClient.prompt()
                .user(prompt)
                .call()
                .content();

        String risco = extrairRisco(resposta);
        String justificativa = extrairJustificativa(resposta);

        return new PedidoClassificado(pedido, risco, justificativa);
    }

    private String extrairRisco(String resposta) {
        for (String linha : resposta.split("\n")) {
            if (linha.toUpperCase().contains("RISCO:")) {
                String valor = linha.substring(linha.indexOf(":") + 1).trim();
                if (valor.contains("ALTO")) return "ALTO";
                if (valor.contains("MEDIO") || valor.contains("MÉDIO")) return "MEDIO";
                if (valor.contains("BAIXO")) return "BAIXO";
            }
        }
        return "INDEFINIDO";
    }

    private String extrairJustificativa(String resposta) {
        for (String linha : resposta.split("\n")) {
            if (linha.toUpperCase().contains("JUSTIFICATIVA:")) {
                return linha.substring(linha.indexOf(":") + 1).trim();
            }
        }
        return resposta.trim();
    }
}
