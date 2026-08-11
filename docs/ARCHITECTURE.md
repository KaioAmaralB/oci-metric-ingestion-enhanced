# Arquitetura e decisões de desenvolvimento

## 1. Fluxo da solução

```text
OCI Monitoring
      |
      v
OCI Connector Hub
      |
      v
OCI Function
      |
      v
Dynatrace Metrics API v2
```

O projeto mantém a arquitetura direta do laboratório. Não foram adicionados OCI Streaming, Kafka, OpenTelemetry Collector, banco de estado ou DLQ no caminho principal.

Essa escolha reduz componentes, mas mantém as limitações de uma integração síncrona e at-least-once.

## 2. Unidade de entrada

O Connector Hub envia um objeto JSON ou uma lista de objetos. Cada objeto representa, de forma prática:

```text
namespace
+ nome da métrica
+ combinação de dimensões
+ um ou mais datapoints
```

Um objeto não equivale necessariamente a um único datapoint. Por isso, o batch configurado no Connector Hub não determina diretamente a quantidade de linhas ou requests ao Dynatrace.

## 3. Etapas executadas pela Function

O handler atual segue estas fases:

```text
1. ler o payload do FDK;
2. converter objeto único em lista, quando necessário;
3. validar se todos os registros são objetos JSON;
4. contar records/datapoints e registrar invocation_start;
5. transformar cada registro em linhas MINT;
6. preservar todas as linhas geradas, sem deduplicação textual;
7. calcular deadline local com FUNCTION_TIMEOUT_SECONDS;
8. criar configuração de proxy, quando aplicável;
9. dividir linhas por bytes e quantidade;
10. enviar chunks sequencialmente;
11. registrar invocation_end ou invocation_failed.
```

A transformação completa ocorre antes do primeiro POST. Isso evita envio parcial causado por um erro local de parsing ou mapping.

## 4. Modos de transformação

### 4.1 Modo genérico

Ativado por:

```text
IMPORT_ALL_METRICS=True
```

A metric key é construída como:

```text
cloud.oci.<namespace-sem-oci_>.<nome-da-métrica-OCI>
```

Os datapoints são agrupados por minuto UTC e enviados como gauge summary:

```text
gauge,min=<min>,max=<max>,sum=<soma>,count=<quantidade>
```

### 4.2 Modo curado

Ativado por:

```text
IMPORT_ALL_METRICS=False
```

O código consulta:

```python
namespace_map.get(namespace)
```

O arquivo `metric_mapping.py` precisa definir `namespace_map` e manter compatibilidade com as funções de agregação.

O pacote recebido contém esse arquivo vazio; ele deve ser restaurado antes da entrega.

## 5. Agregação por minuto e multiplicidade

`aggregation.py` aceita:

- epoch em segundos;
- epoch em milissegundos;
- timestamp ISO-8601.

Em cada objeto recebido, os datapoints são agrupados pelo minuto UTC do
timestamp. A Function não espera o fechamento do minuto e não mantém estado
entre invocações.

Cada datapoint pode conter `value` e `count`:

```json
{
  "timestamp": 1786419199000,
  "value": 1,
  "count": 8
}

O campo count representa quantas ocorrências daquele valor foram consolidadas
pelo OCI Monitoring.

Para cada minuto, a Function calcula:

```text
min   = min(value)
max   = max(value)
sum   = Σ(value × count)
count = Σ(count)
```

Quando o campo count não estiver presente, assume-se count = 1.
A multiplicidade é processada matematicamente; a Function não cria uma cópia
do valor para cada ocorrência. Isso mantém o processamento eficiente mesmo
quando count é elevado.

## 6. Serialização MINT

`MintMetric` produz uma linha no formato:

```text
metric.key,dimensao="valor" gauge,<valor-ou-summary> <timestamp-ms>
```

A implementação:

- remove dimensões vazias;
- converte chaves de dimensão para minúsculas;
- ordena dimensões;
- escapa aspas, barras e quebras de linha;
- sempre envia o formato `gauge`.

A ordenação determinística facilita testes, comparação de payloads e
investigação operacional.

## 7. Preservação de linhas e semântica de entrega

A Function não utiliza a linha MINT final como chave de deduplicação.

Linhas textualmente iguais podem ter sido geradas por objetos de origem
distintos e representar parcelas legítimas da mesma métrica, recurso e minuto.
Descartar uma dessas linhas provoca subcontagem.

Por isso, todas as linhas produzidas pela transformação são preservadas e
encaminhadas ao cliente HTTP.

A solução continua trabalhando com semântica at-least-once:

```text
falha ou timeout
        ↓
Connector Hub pode repetir a invocação
        ↓
linhas previamente aceitas podem ser reenviadas
```

A Function não implementa exactly-once nem deduplicação persistente entre
invocações. Para isso seria necessário um identificador de origem confiável e
um armazenamento durável de estado.

## 8. Chunking HTTP

`dynatrace_client.py` fecha um chunk quando o primeiro limite é atingido:

```text
MAX_PAYLOAD_BYTES
ou
MAX_LINES_PER_REQUEST
```

O tamanho considera bytes UTF-8 e as quebras de linha.

Configuração de homologação:

```text
MAX_PAYLOAD_BYTES=524288
MAX_LINES_PER_REQUEST=5000
```

O limite conservador fica abaixo do máximo de 1 MB da Metrics API v2.

## 9. Reutilização de conexão

Uma `requests.Session` global é criada no import do módulo e montada com `HTTPAdapter`:

```text
pool_connections=20
pool_maxsize=20
max_retries=0
```

O retry do adapter é desabilitado porque o código implementa sua própria política.

O `DynatraceClient` também é mantido em cache no container aquecido. A assinatura da configuração é recalculada a cada invocação; quando muda, um novo cliente é criado.

## 10. Autenticação

### API token

Cabeçalho:

```text
Authorization: Api-Token <token>
```

### OAuth

O cliente solicita token em:

```text
https://sso.dynatrace.com/sso/oauth2/token
```

O token fica em memória no container e é renovado 60 segundos antes da expiração.

A renovação OAuth usa os mesmos timeouts base, mas não possui retry específico por status HTTP.

## 11. Retry e classificação de erros

Status transitórios:

```text
408, 425, 429, 500, 502, 503, 504
```

Erros de rede transitórios:

```text
ConnectTimeout
ReadTimeout
ProxyError
SSLError
ConnectionError
```

O código aplica:

```text
backoff exponencial
+ jitter
+ Retry-After quando disponível
```

Após `HTTP_MAX_ATTEMPTS`, a exceção é propagada para o handler e a invocação falha.

Outros status HTTP são tratados como permanentes.

## 12. HTTP 400 parcial

A Metrics API pode aceitar linhas válidas e rejeitar apenas parte do payload. A implementação atual:

1. lê `linesOk` e `linesInvalid` quando disponíveis;
2. registra `dynatrace_line_rejected` por número de linha;
3. não reenvia o chunk inteiro;
4. continua a invocação.

Essa decisão evita duplicar linhas já aceitas, mas não oferece DLQ nem replay das linhas inválidas.

## 13. Deadline

O handler usa um deadline local:

```text
início da execução + FUNCTION_TIMEOUT_SECONDS
```

O cliente reduz connect/read timeout quando o tempo restante fica curto e reserva `FUNCTION_SAFETY_MARGIN_SECONDS`.

A implementação atual não consulta `ctx.Deadline()` do FDK. Por isso:

```text
FUNCTION_TIMEOUT_SECONDS
```

deve permanecer alinhado ao timeout configurado em `func.yaml` ou na Function.

## 14. Observabilidade interna

Eventos emitidos:

```text
invocation_start
mapping_not_found
metric_not_mapped
dynatrace_oauth
dynatrace_http
dynatrace_network_error
dynatrace_validation_error
dynatrace_line_rejected
invocation_end
invocation_failed
```

Métricas de runtime registradas no fim:

```text
total_ms
transform_ms
cpu_ms
current_rss_mb
peak_rss_mb
function_memory_limit_mb
chunks
http_attempts
retries
http_total_ms
http_max_ms
lines_ok
lines_invalid
payload_bytes
```

`peak_rss_mb` representa o pico do processo desde o início do container, não somente da invocação.

## 15. Semântica de entrega

O desenho é at-least-once.

Cenário de duplicidade:

```text
chunk 1 aceito
chunk 2 falha
Function falha
Connector Hub repete o lote
chunk 1 pode ser reenviado
```

Cenário de rejeição permanente:

```text
chunk retorna 400 parcial
linhas válidas permanecem aceitas
linhas inválidas são apenas registradas
```

Não existe transação distribuída entre Connector Hub, Function e Dynatrace.

## 16. Isolamento por namespace

O parser aceita vários namespaces no mesmo lote. Mesmo assim, para cargas ou SLOs diferentes, recomenda-se:

```text
Connector Hub Métricas A -> Function Métricas A
Connector Hub Métricas B -> Function Métricas B
```

A separação isola:

- backlog;
- timeout;
- retries;
- DataFreshness;
- capacidade;
- incidentes e rollback.

## 17. Limites do desenho direto

Não estão disponíveis:

- exactly-once;
- DLQ durável;
- replay controlado;
- buffer independente da Function;
- reprocessamento histórico com novo mapping;
- isolamento de indisponibilidade longa do Dynatrace.

Quando esses requisitos forem mandatórios:

```text
OCI Monitoring
      -> Connector Hub
      -> OCI Streaming
      -> consumidor/exportador
      -> Dynatrace
```
