package com.rafaelperracini.kafkaailab.gateway;

public interface OllamaGateway {

    String chat(String systemPrompt, String userMessage);
}
