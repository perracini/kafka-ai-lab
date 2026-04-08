package com.rafaelperracini.kafkaailab.config;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.Semaphore;

@Component
public class RateLimiterConfig {

    private static final Logger log = LoggerFactory.getLogger(RateLimiterConfig.class);
    private static final int MAX_CONCURRENT_AI_CALLS = 2;

    private final Semaphore semaphore = new Semaphore(MAX_CONCURRENT_AI_CALLS);
    private final Counter rejectedCounter;

    public RateLimiterConfig(MeterRegistry meterRegistry) {
        this.rejectedCounter = Counter.builder("ai.chat.rejected")
                .description("Chamadas rejeitadas por rate limiting")
                .register(meterRegistry);
    }

    public <T> T executar(java.util.function.Supplier<T> chamada) {
        if (!semaphore.tryAcquire()) {
            rejectedCounter.increment();
            log.warn("Rate limit atingido — {} chamadas simultaneas em andamento", MAX_CONCURRENT_AI_CALLS);
            throw new RateLimitExceededException("Limite de chamadas simultaneas atingido. Tente novamente em instantes.");
        }
        try {
            return chamada.get();
        } finally {
            semaphore.release();
        }
    }

    public static class RateLimitExceededException extends RuntimeException {
        public RateLimitExceededException(String message) {
            super(message);
        }
    }
}
