package com.rafaelperracini.kafkaailab.gateway.impl;

import com.rafaelperracini.kafkaailab.gateway.OllamaGateway;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.stereotype.Component;

@Component
public class OllamaGatewayImpl implements OllamaGateway {

    private final ChatClient chatClient;

    public OllamaGatewayImpl(ChatClient.Builder chatClientBuilder) {
        this.chatClient = chatClientBuilder.build();
    }

    @Override
    public String chat(String systemPrompt, String userMessage) {
        return chatClient.prompt()
                .system(systemPrompt)
                .user(userMessage)
                .call()
                .content();
    }
}
