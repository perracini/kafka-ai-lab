package com.rafaelperracini.kafkaailab.controller;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.concurrent.TimeUnit;

@RestController
@RequestMapping("/metricas")
public class MetricasController {

    private final MeterRegistry meterRegistry;

    public MetricasController(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    @GetMapping("/ia")
    public Map<String, Object> metricasIa() {
        Timer timer = meterRegistry.find("ai.chat.duration").timer();
        double successCount = meterRegistry.find("ai.chat.calls").tag("status", "success")
                .counter() != null ? meterRegistry.find("ai.chat.calls").tag("status", "success").counter().count() : 0;
        double errorCount = meterRegistry.find("ai.chat.calls").tag("status", "error")
                .counter() != null ? meterRegistry.find("ai.chat.calls").tag("status", "error").counter().count() : 0;
        double rejectedCount = meterRegistry.find("ai.chat.rejected")
                .counter() != null ? meterRegistry.find("ai.chat.rejected").counter().count() : 0;

        return Map.of(
                "totalChamadas", (long) (successCount + errorCount),
                "chamadaComSucesso", (long) successCount,
                "chamadaComErro", (long) errorCount,
                "chamadaRejeitadaRateLimit", (long) rejectedCount,
                "latenciaMediaMs", timer != null ? Math.round(timer.mean(TimeUnit.MILLISECONDS)) : 0,
                "latenciaMaxMs", timer != null ? Math.round(timer.max(TimeUnit.MILLISECONDS)) : 0
        );
    }
}
