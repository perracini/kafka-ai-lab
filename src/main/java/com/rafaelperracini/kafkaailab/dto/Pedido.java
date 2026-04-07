package com.rafaelperracini.kafkaailab.dto;

public record Pedido(
    String id,
    String cliente,
    double valor,
    String descricao,
    int quantidadeItens
) {}
