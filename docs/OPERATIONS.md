# Runbook operacional

## 1. Objetivo

Este runbook permite identificar em qual camada ocorre o problema:

```text
OCI Monitoring
Connector Hub
OCI Function
rede/proxy/NAT
Dynatrace
```

Use sempre a mesma janela UTC em todas as telas.

## 2. Eventos de log disponíveis

Os eventos do código são JSON dentro de `data.message`.

### Início da invocação

```text
search "<ESCOPO_DO_LOG>"
| where data.message = '*"invocation_start"*'
| sort by datetime desc
| select datetime, data.opcRequestId, data.message
```

Campos principais:

```text
records_in
datapoints_in
input_bytes
current_rss_mb
peak_rss_mb
oldest_source_age_seconds
```

`oldest_source_age_seconds` pode ficar ausente quando o payload usa timestamp ISO-8601, porque a telemetria atual só considera timestamps numéricos.

### Fim da invocação

```text
search "<ESCOPO_DO_LOG>"
| where data.message = '*"invocation_end"*'
| sort by datetime desc
| select datetime, data.opcRequestId, data.message
```

Campos principais:

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

### Falha propagada

```text
search "<ESCOPO_DO_LOG>"
| where data.message = '*"invocation_failed"*'
| sort by datetime desc
| select datetime, data.opcRequestId, data.message
```

### Resultado HTTP

```text
search "<ESCOPO_DO_LOG>"
| where data.message = '*"dynatrace_http"*'
| sort by datetime desc
| select datetime, data.opcRequestId, data.message
```

Campos:

```text
chunk
attempt
status
lines
payload_bytes
elapsed_ms
lines_ok
lines_invalid
metric_keys
```

### Erros de rede

```text
search "<ESCOPO_DO_LOG>"
| where data.message = '*"dynatrace_network_error"*'
| sort by datetime desc
| select datetime, data.opcRequestId, data.message
```

Fases possíveis:

```text
connect_timeout
read_timeout
proxy
tls
connection
```

### Rejeições de linha

```text
search "<ESCOPO_DO_LOG>"
| where data.message = '*"dynatrace_line_rejected"*'
| sort by datetime desc
| select datetime, data.opcRequestId, data.message
```

### OAuth

```text
search "<ESCOPO_DO_LOG>"
| where data.message = '*"dynatrace_oauth"*'
| sort by datetime desc
| select datetime, data.opcRequestId, data.message
```

## 3. Correlação

A equipe deve usar:

```text
data.opcRequestId
```

O helper `_request_id()` atual não consulta `ctx.CallID()`, portanto o campo `request_id` dentro do JSON pode ficar vazio.

Procedimento:

1. copie `data.opcRequestId` de uma linha `Served function invocation...`;
2. filtre todos os logs pelo mesmo valor;
3. ordene por `datetime asc`;
4. reconstrua a sequência `invocation_start -> dynatrace_http -> invocation_end/failed`.

## 4. Métricas do Connector Hub

Namespace:

```text
oci_service_connector_hub
```

| Métrica | Estatística sugerida | Interpretação |
|---|---|---|
| `DataFreshness` | Max / 1m | idade do registro mais recente processado |
| `LatencyAtSource` | P90 e Max / 1m | leitura do OCI Monitoring |
| `LatencyAtTarget` | P90 e Max / 1m | tempo de espera pelo target Function |
| `MessagesReadFromSource` | Max / 1m | leitura; pode acumular durante retry |
| `MessagesWrittenToTarget` | Sum / 1m | registros entregues à Function |
| `ErrorsAtTarget` | Sum por erro | falhas reconhecidas pelo Connector Hub |

### DataFreshness

O valor deve ser baixo e, principalmente, estável.

Referência operacional depende da sua necessidade de intervalo. Caso o requisito seja de 1 minuto, DataFreshness tem que estar abaixo disso.

### Leitura dos gráficos

```text
LatencyAtSource baixa
+ LatencyAtTarget alta
= gargalo no target/Function
```

```text
DataFreshness crescente
+ MessagesWrittenToTarget caindo
= backlog aumentando
```

```text
ErrorsAtTarget = 0
+ Dynatrace com lacunas
= possível erro HTTP engolido, HTTP 400 parcial ou falta de dado na origem
```

Na versão atual, falhas transitórias finais são propagadas; HTTP 400 parcial é tratado como concluído.

## 5. Métricas da Function

Namespace:

```text
oci_faas
```

| Métrica | Estatística sugerida |
|---|---|
| `FunctionExecutionDuration` | P90/P95 e Max |
| `FunctionInvocationCount` | Sum |
| `FunctionResponseCount` | Sum por `responseType`, `errorCode`, `errorMessage` |
| `AllocatedTotalConcurrency` | Max por Application, quando disponível |

`AllocatedTotalConcurrency` mostra memória alocada para concorrência no nível da Application; não representa RSS real da Function.

RSS real aproximado é registrado pelo código:

```text
current_rss_mb
peak_rss_mb
```

## 6. Como localizar o gargalo

### 6.1 Dynatrace/rede

Sinais:

```text
http_total_ms próximo de total_ms
cpu_ms muito menor que total_ms
read_timeout/connect_timeout
HTTP 429/5xx
```

Interpretação:

| Evidência | Diagnóstico provável |
|---|---|
| `connect_timeout` | rota, NAT, proxy, firewall, DNS/TCP/TLS |
| `read_timeout` | conexão estabelecida, resposta remota lenta |
| `proxy` | proxy inacessível ou autenticação |
| `tls` | certificado, CA, inspeção TLS ou SNI |
| `429` | throttling remoto |
| `5xx` | erro temporário do endpoint |
| HTTP rápido e total lento | transformação, logging ou muitos chunks |

A versão atual não possui probe DNS/TCP/TLS. Para aprofundar, use:

- VCN Flow Logs;
- métricas do NAT Gateway;
- logs do proxy;
- teste `curl` a partir de uma VM na mesma rota;

### 6.2 CPU

Compare:

```text
cpu_ms
versus
total_ms
```

Exemplo:

```text
total_ms=10000
cpu_ms=300
```

A maior parte do tempo foi espera de I/O.

Exemplo:

```text
total_ms=10000
cpu_ms=8500
```

Investigue transformação, serialização, payload e volume de logs.

### 6.3 Memória

Campos:

```text
current_rss_mb
peak_rss_mb
function_memory_limit_mb
```

`peak_rss_mb` é o pico do processo desde o início do container aquecido.

Orientação inicial:

```text
pico < 70% do limite: margem confortável
70–85%: investigar sob carga
> 85%: risco operacional
crescimento contínuo no mesmo container: possível retenção/vazamento
```

O código atual não calcula o percentual; faça a conta externamente:

```text
peak_rss_mb / function_memory_limit_mb * 100
```

## 7. Resposta a 504

1. filtre `FunctionResponseCount` por `errorCode`;
2. confirme se é `FunctionInvokeTimeout`;
3. localize `invocation_failed` e os últimos `dynatrace_http` do mesmo `opcRequestId`;
4. compare `http_total_ms`, `http_max_ms`, `cpu_ms` e `total_ms`;
5. verifique `DataFreshness`;
6. conte chunks e retries;
7. não aumente o timeout antes de identificar a fase dominante.

Interpretação comum:

```text
http_total_ms alto
+ read_timeout
= Dynatrace/proxy lento ou muitos requests/chunks
```

```text
transform_ms alto
+ cpu_ms alto
= transformação local
```

## 8. Resposta a 403

HTTP 403 comprova que houve resposta HTTP. Verifique:

```text
DYNATRACE_TENANT
AUTH_METHOD
credencial pertencente ao mesmo ambiente
escopo/política de ingestão
```

URL SaaS:

```text
https://<environment-id>.live.dynatrace.com
```

Não use `.apps.dynatrace.com`.

## 9. Resposta a 429

1. identifique `status=429` em `dynatrace_http`;
2. confira a quantidade de `retries` no `invocation_end`;
3. confirme se a Function termina antes do deadline;
4. reduza requests aumentando eficiência do chunk, sem exceder 1 MB;
5. avalie separar Cache e Streaming;
6. valide limites e consumo no Dynatrace.

A versão atual usa `Retry-After`, mas não emite evento separado informando o delay.

## 10. HTTP 400 parcial

A implementação atual considera o chunk concluído após registrar as linhas inválidas.

Ações:

1. consultar `dynatrace_line_rejected`;
2. identificar metric key e motivo;
3. comparar timestamp e dimensões;
4. corrigir a origem/mapping;
5. confirmar que `lines_invalid` voltou a zero.

Não existe DLQ nem reenvio individual.

## 11. Verificação do NAT Gateway

Quando a Function usa subnet privada e saída direta:

```text
Function -> NAT Gateway -> internet -> Dynatrace
```

Acompanhe no namespace do NAT:

```text
DropsToNATgw
highPortUsageWatermark
ConnectionsEstablished
ConnectionsClosed
ConnectionsTimedOut
BytesToNATgw
BytesFromNATgw
```

Sinais:

```text
dropType=noPorts > 0
= pressão/exaustão de portas SNAT
```

```text
highPortUsageWatermark = 1
= uso elevado de portas
```

A reutilização de `requests.Session` reduz esse risco.

## 13. Verificação com VCN Flow Logs

Ative Flow Logs na subnet da Application e filtre TCP/443.

Interpretação:

```text
REJECT
= NSG ou Security List
```

```text
ACCEPT + connect_timeout
= investigar NAT, proxy, rota após a VNIC ou endpoint remoto
```

Flow Logs não exibem status HTTP nem tempo da aplicação.

## 15. Separação por namespace

Para diagnóstico e produção crítica:

```text
Connector Hub Métrica A -> Function Métrica A
Connector Hub Métrica B -> Function Métrica B
```

Use a mesma imagem, mas Functions e métricas independentes.

Isso permite comparar:

```text
DataFreshness
FunctionExecutionDuration
HTTP status
RSS
retries
```

sem misturar cargas.
