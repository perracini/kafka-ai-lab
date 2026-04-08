# Kafka AI Lab

Microsserviço Spring Boot que integra Kafka com IA local (Ollama) para classificação de risco de pedidos.

**Sem Docker.** O Kafka sobe programaticamente junto com a aplicação (Embedded Kafka).

## Stack

- Java 17+ / Spring Boot 3.5.0
- Spring Kafka + Kafka embutido (spring-kafka-test em escopo compile)
- Spring AI 1.1.4 + Ollama com Llama 3.2 (3B)
- Spring Boot Actuator + Micrometer (observabilidade)
- Spring Retry (retry com backoff exponencial + fallback)
- Caffeine Cache (cache de respostas da IA)
- Semaphore-based Rate Limiter (limite de chamadas concorrentes)
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

---

## Conceito: Observabilidade e Resiliencia em chamadas de IA

Chamar um LLM local (Ollama) ou remoto (API) pode falhar, demorar ou sobrecarregar o sistema.
Este projeto implementa 4 camadas de protecao:

### 1. Metricas customizadas (Micrometer)

O `OllamaGatewayImpl` registra metricas automaticamente a cada chamada:

- **`ai.chat.duration`** (Timer) — latencia de cada chamada ao Ollama
- **`ai.chat.calls`** (Counter, tag `status=success|error`) — total de chamadas com sucesso/erro
- **`ai.chat.rejected`** (Counter) — chamadas rejeitadas pelo rate limiter

Essas metricas ficam disponiveis via Actuator (`/actuator/metrics/ai.chat.duration`) e tambem
num endpoint customizado `GET /metricas/ia` que retorna um resumo consolidado.

### 2. Retry com fallback (Spring Retry)

```java
@Retryable(maxAttempts = 3, backoff = @Backoff(delay = 2000, multiplier = 2))
public String chat(String systemPrompt, String userMessage) { ... }

@Recover
public String chatFallback(Exception e, String systemPrompt, String userMessage) {
    return "RISCO: INDEFINIDO\nJUSTIFICATIVA: Servico de IA indisponivel...";
}
```

- **3 tentativas** com backoff exponencial: 2s, 4s, 8s
- Se todas falharem, o `@Recover` retorna uma classificacao segura ("INDEFINIDO")
- O consumer Kafka nao quebra — o pedido e classificado como pendente de revisao manual

### 3. Cache de respostas (Caffeine)

```java
@Cacheable(value = "classificacoes",
           key = "#pedido.descricao() + '|' + #pedido.valor() + '|' + #pedido.quantidadeItens()")
```

- **Key**: `descricao|valor|quantidadeItens` — mesmo padrao de pedido = mesmo risco, evita chamada redundante a IA
- **Separador `|`** na key evita colisoes por concatenacao (ex: `"abc" + "10.0"` vs `"abc1" + "0.0"`)
- **TTL**: 300s (5 min) via `expireAfterWrite` — o cache expira automaticamente
- **Max entries**: 100 — evita consumo excessivo de memoria

**Sobre invalidacao do cache:** a estrategia atual e TTL automatico (`expireAfterWrite=300s`),
sem necessidade de endpoint manual. Para cenarios mais complexos em producao, alternativas incluem:
- `expireAfterAccess` — expira apos N segundos sem acesso (entradas populares vivem mais)
- Event-driven — um topico Kafka `modelo-atualizado` dispara `@CacheEvict(allEntries = true)` quando o modelo de IA muda
- `refreshAfterWrite` — refresh assincrono em background (requer `CacheLoader`)

**Nota:** a key do cache exclui propositalmente o campo `cliente`. Para este exercicio, a classificacao
de risco depende do padrao do pedido (produto/valor/quantidade), nao de quem comprou. Em um sistema
de fraud detection real, o historico e perfil do cliente seriam fatores relevantes e deveriam
compor a chave do cache ou invalidar entries associadas.

### 4. Rate Limiting (Semaphore)

```java
private static final int MAX_CONCURRENT_AI_CALLS = 2;
private final Semaphore semaphore = new Semaphore(MAX_CONCURRENT_AI_CALLS);
```

- Maximo de **2 chamadas simultaneas** ao Ollama (LLM local e single-threaded, mais que isso nao ajuda)
- Se o limite for atingido, a chamada e rejeitada imediatamente (`tryAcquire`, nao-bloqueante)
- Chamadas rejeitadas sao contabilizadas na metrica `ai.chat.rejected`

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

### GET /metricas/ia — Metricas customizadas da IA

Retorna um resumo consolidado das metricas de chamadas ao Ollama.

**Resposta:**
```json
{
  "totalChamadas": 1,
  "chamadaComSucesso": 1,
  "chamadaComErro": 0,
  "chamadaRejeitadaRateLimit": 0,
  "latenciaMediaMs": 8349,
  "latenciaMaxMs": 8349
}
```

### Actuator endpoints

- `GET /actuator/health` — status da aplicacao (inclui Kafka e disco)
- `GET /actuator/metrics` — lista de todas as metricas disponiveis
- `GET /actuator/metrics/ai.chat.duration` — latencia detalhada das chamadas a IA

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
│   ├── EmbeddedKafkaConfig.java             # Kafka embutido (sem Docker)
│   └── RateLimiterConfig.java               # Rate limiter via Semaphore (max 2 chamadas)
├── controller/
│   ├── PedidoController.java               # POST /pedidos, GET /pedidos/classificados
│   └── MetricasController.java             # GET /metricas/ia — resumo de metricas da IA
├── dto/
│   ├── Pedido.java                          # Record — dados do pedido
│   └── PedidoClassificado.java              # Record — pedido + risco + justificativa
├── gateway/
│   ├── OllamaGateway.java                  # Interface — chamadas ao Ollama (LLM)
│   ├── PedidoKafkaGateway.java             # Interface — publicação nos tópicos Kafka
│   └── impl/
│       ├── OllamaGatewayImpl.java          # Implementação — ChatClient + métricas + retry + fallback
│       └── PedidoKafkaGatewayImpl.java     # Implementação — KafkaTemplate + ObjectMapper
├── kafka/
│   └── PedidoConsumer.java                  # Listener — desserializa e delega ao Service
├── repository/
│   ├── PedidoClassificadoRepository.java   # Interface — armazenamento de resultados
│   └── impl/
│       └── PedidoClassificadoRepositoryImpl.java  # Implementação — in-memory (CopyOnWriteArrayList)
└── service/
    ├── PedidoService.java                   # Interface — criação, consulta e processamento
    ├── ClassificadorRiscoService.java       # Interface — classificação de risco
    └── impl/
        ├── PedidoServiceImpl.java           # Implementação — orquestra gateways + repository
        └── ClassificadorRiscoServiceImpl.java  # Implementação — delega ao OllamaGateway + cache
```

## Arquitetura

- **Controller** — apenas HTTP, sem lógica. Delega ao PedidoService.
- **Service (PedidoService)** — orquestra criação de ID, publicação via gateway, classificação e armazenamento.
- **Service (ClassificadorRisco)** — regra de negócio: monta prompt e interpreta resposta da IA.
- **Gateway (OllamaGateway)** — encapsula chamadas ao LLM (Ollama/Llama 3.2) com metricas, retry e fallback.
- **Gateway (PedidoKafkaGateway)** — encapsula publicação nos tópicos Kafka.
- **Config (RateLimiterConfig)** — limita chamadas concorrentes a IA via Semaphore.
- **Repository** — armazena pedidos classificados (in-memory para dev).
- **Consumer** — listener Kafka: apenas desserializa e delega ao Service.
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
