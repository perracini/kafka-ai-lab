package com.rafaelperracini.kafkaailab.service;

import com.rafaelperracini.kafkaailab.dto.Pedido;
import com.rafaelperracini.kafkaailab.dto.PedidoClassificado;

public interface ClassificadorRiscoService {

    PedidoClassificado classificar(Pedido pedido);
}
