package com.rafaelperracini.kafkaailab.gateway.impl;

import com.rafaelperracini.kafkaailab.config.RateLimiterConfig;
import com.rafaelperracini.kafkaailab.gateway.OllamaGateway;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;

@Component
public class OllamaGatewayImpl implements OllamaGateway {

    private static final Logger log = LoggerFactory.getLogger(OllamaGatewayImpl.class);

    private final ChatClient chatClient;
    private final RateLimiterConfig rateLimiter;
    private final Timer aiTimer;
    private final Counter aiSuccessCounter;
    private final Counter aiErrorCounter;

    public OllamaGatewayImpl(ChatClient.Builder chatClientBuilder,
                             RateLimiterConfig rateLimiter,
                             MeterRegistry meterRegistry) {
        this.chatClient = chatClientBuilder.build();
        this.rateLimiter = rateLimiter;

        this.aiTimer = Timer.builder("ai.chat.duration")
                .description("Tempo de resposta das chamadas ao Ollama")
                .tag("model", "llama3.2")
                .register(meterRegistry);

        this.aiSuccessCounter = Counter.builder("ai.chat.calls")
                .description("Total de chamadas ao Ollama")
                .tag("status", "success")
                .register(meterRegistry);

        this.aiErrorCounter = Counter.builder("ai.chat.calls")
                .description("Total de chamadas ao Ollama")
                .tag("status", "error")
                .register(meterRegistry);
    }

    @Override
    @Retryable(maxAttempts = 3, backoff = @Backoff(delay = 2000, multiplier = 2))
    public String chat(String systemPrompt, String userMessage) {
        return rateLimiter.executar(() -> aiTimer.record(() -> {
            try {
                String resposta = chatClient.prompt()
                        .system(systemPrompt)
                        .user(userMessage)
                        .call()
                        .content();
                aiSuccessCounter.increment();
                return resposta;
            } catch (Exception e) {
                aiErrorCounter.increment();
                log.error("Erro na chamada ao Ollama: {}", e.getMessage());
                throw e;
            }
        }));
    }

    @Recover
    public String chatFallback(Exception e, String systemPrompt, String userMessage) {
        log.warn("Fallback ativado apos falha nas tentativas. Erro: {}", e.getMessage());
        return "RISCO: INDEFINIDO\nJUSTIFICATIVA: Servico de IA indisponivel no momento. Classificacao pendente de revisao manual.";
    }
}
