package com.rafaelperracini.kafkaailab.service;

import com.rafaelperracini.kafkaailab.dto.Pedido;
import com.rafaelperracini.kafkaailab.dto.PedidoClassificado;

import java.util.List;
import java.util.Map;

public interface PedidoService {

    Map<String, String> criar(Pedido pedido);

    List<PedidoClassificado> listarClassificados();

    void processar(Pedido pedido);
}
