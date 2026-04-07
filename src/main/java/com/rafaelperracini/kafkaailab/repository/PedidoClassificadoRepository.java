package com.rafaelperracini.kafkaailab.repository;

import com.rafaelperracini.kafkaailab.dto.PedidoClassificado;

import java.util.List;

public interface PedidoClassificadoRepository {

    void salvar(PedidoClassificado classificado);

    List<PedidoClassificado> listarTodos();
}
