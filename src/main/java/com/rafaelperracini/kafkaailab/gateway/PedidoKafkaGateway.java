package com.rafaelperracini.kafkaailab.gateway;

import com.rafaelperracini.kafkaailab.dto.Pedido;
import com.rafaelperracini.kafkaailab.dto.PedidoClassificado;

public interface PedidoKafkaGateway {

    void publicarPedido(Pedido pedido);

    void publicarClassificacao(String key, PedidoClassificado classificado);
}
