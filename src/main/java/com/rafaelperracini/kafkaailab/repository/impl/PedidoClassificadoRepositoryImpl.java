package com.rafaelperracini.kafkaailab.repository.impl;

import com.rafaelperracini.kafkaailab.dto.PedidoClassificado;
import com.rafaelperracini.kafkaailab.repository.PedidoClassificadoRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Repository
public class PedidoClassificadoRepositoryImpl implements PedidoClassificadoRepository {

    private final List<PedidoClassificado> resultados = new CopyOnWriteArrayList<>();

    @Override
    public void salvar(PedidoClassificado classificado) {
        resultados.add(classificado);
    }

    @Override
    public List<PedidoClassificado> listarTodos() {
        return List.copyOf(resultados);
    }
}
