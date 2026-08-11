# Guia de manutenção e evolução

## 1. Objetivo

Este guia explica onde alterar o projeto sem transformar a Function em outro produto. A divisão principal do código permanece próxima ao laboratório original.

## 2. Responsabilidade de cada arquivo

| Arquivo | Responsabilidade atual |
|---|---|
| `func.py` | entrypoint FDK, parsing, transformação, dimensões, proxy, telemetria e orquestração |
| `dynatrace_client.py` | autenticação, chunking, conexão HTTP, retry e validação de resposta |
| `aggregation.py` | conversão de timestamp e agregação por minuto |
| `metric_mapping.py` | catálogo curado OCI -> Dynatrace; precisa definir `namespace_map` |
| `mint.py` | serialização de uma linha MINT |
| `summary_stat.py` | representação de min/max/sum/count |
| `func.yaml` | runtime, recursos e configuração do ambiente |

## 3. Fluxo de chamada real

```text
handler()
  -> _metric_lines(record)
       -> create_minutely_summary_buckets()
       ou namespace_map
       -> MintMetric
  -> _client()
       -> DynatraceClient.send_metric_lines()
            -> _chunks()
            -> _send_chunk()
                 -> _authorization()
                 -> requests.Session.post()
```

## 4. Como adicionar um namespace no modo genérico

Com:

```text
IMPORT_ALL_METRICS=True
```

normalmente não é necessário adicionar mapping. O nome da métrica será construído automaticamente.

Revise, porém:

- se a metric key resultante é válida no Dynatrace;
- se as dimensões fixas são suficientes;
- se a cardinalidade é aceitável;
- se a unidade e a semântica de gauge são adequadas.

Para incluir uma dimensão, altere `GENERIC_DIMENSIONS` e crie teste.

Evite dimensões voláteis, como request ID, offset, timestamp ou IDs temporários.

## 5. Como adicionar ou restaurar mapping curado

`metric_mapping.py` deve definir:

```python
namespace_map
```

A interface esperada pelo código é:

```python
metric_map.value_from_oci_metric_name(
    metric_name,
    oci_dimensions,
    datapoints,
)

metric_map.dimensions(oci_dimensions)
```

As funções de agregação disponíveis e compatíveis são:

```python
AggregateResult(timestamp, value)
aggregate_max(datapoints)
aggregate_min(datapoints)
aggregate_sum(datapoints)
aggregate_mean(datapoints)
```

Procedimento recomendado:

1. criar ou atualizar a entrada do namespace;
2. definir nome Dynatrace e agregação;
3. mapear somente dimensões estáveis;
4. testar com `IMPORT_ALL_METRICS=False`;
5. comparar valores com o OCI Metrics Explorer;
6. validar continuidade de dashboards antes de renomear keys existentes.

## 6. Como adicionar uma configuração

O projeto atual não possui uma dataclass central de runtime. Para uma nova variável:

1. escolha `_bool`, `_int`, `_float` ou `os.environ.get` em `func.py`;
2. defina default explícito;
3. valide faixa e valores aceitos;
4. adicione o valor não sensível ao `func.yaml`, se necessário;
5. documente em `CONFIGURATION.md`;
6. crie teste de valor válido, inválido e ausente;
7. confirme que a variável realmente é lida pelo código.

## 7. Segredos

Nunca grave no repositório:

```text
DYNATRACE_API_KEY
OAUTH_CLIENT_SECRET
PROXY_PASSWORD
API keys OCI
```

Use `fn config function` ou um mecanismo seguro equivalente.

Antes de abrir pull request:

```bash
grep -RInE 'secret|token|password|dt0s|Api-Token' .
```

Revise manualmente os resultados; não inclua o valor encontrado em tickets ou logs.

## 8. Como adicionar um evento de log

Use `_log()` e um nome de evento estável:

```python
_log(
    logging.INFO,
    "nome_do_evento",
    request_id=request_id,
    metric_key=metric_key,
    duration_ms=duration_ms,
)
```

Regras:

- uma linha JSON por evento;
- não registrar token, secret ou header de autorização;
- evitar payload completo em `INFO`;
- usar milissegundos para duração;
- manter nomes de campos estáveis;
- usar `data.opcRequestId` como correlação operacional;
- atualizar `OPERATIONS.md` quando o evento mudar.

Eventos atuais:

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

## 9. Política de erros

### Transitórios

Devem terminar em exceção após retry limitado:

```text
408
425
429
500
502
503
504
ConnectTimeout
ReadTimeout
ProxyError
SSLError
ConnectionError
falta de tempo para outra tentativa
```

### Permanentes

Falham imediatamente:

```text
401
403
404
URL inválida
credencial ausente
linha individual maior que o chunk
status HTTP não classificado como transitório
```

### HTTP 400 parcial

O código atual registra linhas inválidas e considera o chunk concluído. Não há DLQ.

Qualquer mudança nessa decisão precisa avaliar:

```text
risco de perda da linha inválida
versus
risco de duplicar linhas válidas já aceitas
```

## 10. Chunking

Ao alterar `_chunks()`:

- conte bytes UTF-8, não caracteres;
- inclua uma quebra de linha entre linhas;
- rejeite linha individual maior que o limite;
- preserve a ordem;
- teste fronteiras exatas de bytes e quantidade.

Não ultrapasse o limite do endpoint Dynatrace.

## 11. Retry e deadline

`_backoff()` usa `Retry-After` ou backoff exponencial com jitter.

`_timeouts()` usa o deadline local passado pelo handler.

Ao evoluir esse ponto:

1. prefira o deadline real do FDK;
2. mantenha margem para log e retorno;
3. não permita que retries consumam todo o timeout;
4. teste uma resposta lenta e vários chunks.

## 12. OAuth

O token é cacheado no objeto `DynatraceClient` por container aquecido e renovado 60 segundos antes da expiração.

Ao alterar autenticação:

- nunca registrar client secret ou access token;
- testar expiração/renovação;
- testar 401/403 e indisponibilidade do endpoint SSO;
- avaliar retry específico para OAuth, atualmente inexistente por status.

## 13. Dimensões e escape

O fluxo atual aplica escape em `_safe_dimension()` e novamente em `MintMetric`.

Antes de alterar:

- crie testes com aspas;
- crie testes com barra invertida;
- crie testes com quebra de linha;
- confirme a linha aceita pelo Dynatrace;
- evite escape duplo.

Não renomeie dimensões ou metric keys sem plano de migração.

## 14. Preservação do campo `count`

O campo `count` dos datapoints OCI não pode ser descartado.

Ao alterar a agregação, mantenha obrigatoriamente:

```text
sum   = Σ(value × count)
count = Σ(count)
min   = min(value)
max   = max(value)
```

Não expanda fisicamente um datapoint em count elementos. Utilize soma
ponderada para preservar desempenho e consumo de memória.

## 15. Atualização do projeto upstream

1. crie uma branch de atualização;
2. compare arquivos do upstream com os locais;
3. preserve a interface de `metric_mapping.py`;
4. execute smoke test de importação;
5. execute testes de transformação e HTTP;
6. faça deploy em homologação;
7. execute Cache e Streaming separadamente;
8. execute load test;
9. atualize changelog e documentação;
10. faça rollback testado antes da janela produtiva.

## 16. Limitações arquiteturais

A Function continua sendo target síncrono do Connector Hub. Não há:

- exactly-once;
- transação entre OCI e Dynatrace;
- DLQ;
- replay controlado;
- buffer independente.