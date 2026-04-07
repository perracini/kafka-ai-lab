# Kafka AI Lab

Microsserviço Spring Boot que integra Kafka com IA local (Ollama) para classificação de risco de pedidos.

**Sem Docker.** O Kafka sobe programaticamente junto com a aplicação (Embedded Kafka).

## Stack

- Java 17+ / Spring Boot 3.5.0
- Spring Kafka + Kafka embutido (spring-kafka-test em escopo compile)
- Spring AI 1.1.4 + Ollama com Llama 3.2 (3B)
- Maven (wrapper incluso)

## Como rodar

1. Certifique-se de ter o modelo do Ollama:
   ```
   ollama pull llama3.2
   ```

2. Inicie o Ollama (se não estiver rodando):
   ```
   ollama serve
   ```

3. Suba a aplicação:
   ```
   ./mvnw spring-boot:run
   ```
   O Kafka embutido sobe automaticamente na porta 9092.

4. Teste via Postman ou curl em `http://localhost:8082`

**Porta 8082** — diferente dos outros labs (8080 spring-ai, 8081 langchain4j).

---

## Conceito: Por que Kafka + IA?

Nos projetos anteriores (spring-ai-lab, langchain4j-lab), a IA respondia de forma **síncrona**:
o Postman manda request, espera o Llama processar, recebe resposta.

Com Kafka, o fluxo é **assíncrono**:

```
POST /pedidos (retorna imediato: "enviado ao Kafka")
       ↓
  Tópico "pedidos" (Kafka armazena o evento)
       ↓
  Consumer lê o evento → chama a IA → classifica risco
       ↓
  Tópico "pedidos-classificados" (resultado publicado)
       ↓
GET /pedidos/classificados (consulta resultados quando quiser)
```

### Vantagens do modelo assíncrono com IA

- **Desacoplamento** — o produtor não espera a IA processar (pode levar segundos)
- **Resiliência** — se a IA cair, os pedidos ficam no Kafka até ela voltar
- **Escalabilidade** — pode ter N consumers processando pedidos em paralelo
- **Auditoria** — todo evento fica registrado nos tópicos do Kafka

## Conceito: Kafka embutido (sem Docker)

O `spring-kafka-test` fornece o `EmbeddedKafkaZKBroker` que inicia um broker Kafka
completo em memória. Normalmente é usado para testes, mas aqui usamos em escopo **compile**
para rodar no main da aplicação.

```java
@Bean(initMethod = "afterPropertiesSet", destroyMethod = "destroy")
public EmbeddedKafkaBroker embeddedKafkaBroker() {
    return new EmbeddedKafkaZKBroker(1, true, "pedidos", "pedidos-classificados")
            .kafkaPorts(9092);
}
```

- `1` = número de brokers (1 é suficiente para dev)
- `true` = auto-criar partições
- `"pedidos", "pedidos-classificados"` = tópicos criados automaticamente
- `.kafkaPorts(9092)` = porta fixa (para o bootstrap-servers do application.yml)

**Em produção**, basta remover esta classe e apontar `spring.kafka.bootstrap-servers`
para o cluster Kafka real. O resto do código não muda.

---

## Endpoints

### POST /pedidos — Envia pedido ao Kafka

Publica um pedido no tópico "pedidos". Retorna imediato (assíncrono).

**Body:**
```json
{
  "cliente": "Joao Silva",
  "valor": 15000.00,
  "descricao": "10 iPhones 15 Pro Max",
  "quantidadeItens": 10
}
```

**Resposta (imediata):**
```json
{
  "status": "Pedido enviado ao Kafka",
  "id": "uuid-gerado",
  "info": "Consulte GET /pedidos/classificados em alguns segundos"
}
```

### GET /pedidos/classificados — Consulta resultados

Retorna todos os pedidos já classificados pela IA.

**Resposta:**
```json
[
  {
    "pedido": {
      "id": "uuid",
      "cliente": "Joao Silva",
      "valor": 15000.0,
      "descricao": "10 iPhones 15 Pro Max",
      "quantidadeItens": 10
    },
    "risco": "MEDIO",
    "justificativa": "Grande valor mas sem indicadores claros de fraude."
  }
]
```

---

## Conceito: Integração Kafka + IA (o Consumer)

A integração acontece no `PedidoConsumer`. Este é o ponto onde o evento Kafka encontra a IA:

```java
@KafkaListener(topics = "pedidos", groupId = "kafka-ai-lab")
public void consumir(String mensagem) {
    Pedido pedido = objectMapper.readValue(mensagem, Pedido.class);     // 1. deserializa
    PedidoClassificado classificado = classificadorService.classificar(pedido); // 2. IA classifica
    kafkaTemplate.send("pedidos-classificados", pedido.id(), json);     // 3. republica
}
```

1. **Deserializa** o JSON do tópico em um `Pedido`
2. **Chama o service** que manda os dados para o Llama 3.2 classificar o risco
3. **Publica** o resultado no tópico "pedidos-classificados"

### O prompt de classificação

O `ClassificadorRiscoServiceImpl` monta um prompt com os dados do pedido e pede ao Llama:

```
SYSTEM: "Voce e um analista de risco de fraude em e-commerce.
         Classifique pedidos como ALTO, MEDIO ou BAIXO risco."

USER: "Classifique o risco de fraude deste pedido:
       - Cliente: Joao Silva
       - Valor: R$ 15000.00
       - Descricao: 10 iPhones 15 Pro Max
       - Quantidade de itens: 10"
```

A temperatura é **0.3** (baixa) porque classificação precisa ser determinística.

---

## Estrutura do projeto

```
src/main/java/com/rafaelperracini/kafkaailab/
├── KafkaAiLabApplication.java               # Main
├── config/
│   └── EmbeddedKafkaConfig.java             # Kafka embutido (sem Docker)
├── controller/
│   └── PedidoController.java               # POST /pedidos, GET /pedidos/classificados
├── dto/
│   ├── Pedido.java                          # Record — dados do pedido
│   └── PedidoClassificado.java              # Record — pedido + risco + justificativa
├── kafka/
│   ├── PedidoProducer.java                  # Publica no tópico "pedidos"
│   └── PedidoConsumer.java                  # Consome, classifica com IA, republica
└── service/
    ├── PedidoService.java                   # Interface — criação e consulta de pedidos
    ├── ClassificadorRiscoService.java       # Interface — classificação de risco
    └── impl/
        ├── PedidoServiceImpl.java           # Implementação — orquestra Producer/Consumer
        └── ClassificadorRiscoServiceImpl.java  # Implementação — ChatClient + Ollama
```

## Arquitetura

- **Controller** — apenas HTTP, sem lógica. Delega ao PedidoService.
- **Service (PedidoService)** — orquestra criação de ID, envio ao Producer e consulta de resultados.
- **Service (ClassificadorRisco)** — interface + impl SOLID. A IA fica isolada no ClassificadorRiscoServiceImpl.
- **Producer** — serializa Pedido em JSON e publica no Kafka
- **Consumer** — consome evento, chama service de IA, republica resultado
- **Config** — Kafka embutido como @Bean. Remover esta classe = usar Kafka real.
- **DTOs** — records imutáveis para dados de entrada e saída

## Exemplos de teste no Postman

**Pedido baixo risco:**
```json
POST http://localhost:8082/pedidos
{ "cliente": "Maria Santos", "valor": 89.90, "descricao": "1 livro de receitas", "quantidadeItens": 1 }
```

**Pedido alto risco:**
```json
POST http://localhost:8082/pedidos
{ "cliente": "Usuario123", "valor": 49999.99, "descricao": "50 notebooks gamer para endereco diferente do cadastro", "quantidadeItens": 50 }
```

**Consultar resultados (aguardar ~15-30s para IA processar):**
```
GET http://localhost:8082/pedidos/classificados
```
