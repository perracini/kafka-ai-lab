package com.rafaelperracini.kafkaailab.dto;

public record PedidoClassificado(
    Pedido pedido,
    String risco,
    String justificativa
) {}
