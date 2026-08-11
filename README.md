> **IMPORTANTE**: 
> Este blog foi desenvolvido exclusivamente para fins educacionais e de estudo. Ele fornece um ambiente para que aprendizes possam experimentar e adquirir experiência prática em um cenário controlado. É importante destacar que as configurações e práticas de segurança utilizadas neste laboratório podem não ser adequadas para cenários do mundo real.
> As considerações de segurança para aplicações reais costumam ser muito mais complexas e dinâmicas. Portanto, antes de implementar qualquer uma das técnicas ou configurações demonstradas aqui em um ambiente de produção, é essencial realizar uma avaliação e revisão de segurança abrangente. Essa revisão deve incluir todos os aspectos de segurança, como controle de acesso, criptografia, monitoramento e conformidade, garantindo que o sistema esteja alinhado com as políticas e padrões de segurança da organização.
> A segurança deve sempre ser uma prioridade máxima ao fazer a transição de um ambiente de laboratório para uma implementação no mundo real.


# OCI Monitoring para Dynatrace — Function evoluída

Esta solução exporta métricas do OCI Monitoring para o Dynatrace usando o fluxo:

```text
OCI Monitoring
      -> OCI Connector Hub
      -> OCI Function
      -> Dynatrace Metrics API v2
```

Ela evolui o laboratório público `dynatrace-extensions/oci-metric-ingestion` da Dynatrace sem introduzir novos componentes no caminho principal. A melhoria central é substituir o envio de um request por datapoint por payloads MINT multiline, com conexão HTTP reutilizável, chunking e retry limitado.

## O que o código atual faz

```text
1. recebe um objeto ou uma lista de metric streams do Connector Hub;
2. valida a estrutura básica do JSON;
3. agrupa os datapoints de cada objeto por minuto UTC;
4. preserva a multiplicidade informada pelo campo count do OCI Monitoring;
5. gera linhas MINT com min, max, soma ponderada e contagem total;
6. preserva todas as linhas geradas, inclusive linhas textualmente iguais;
7. divide as linhas por quantidade e tamanho UTF-8;
8. reutiliza uma requests.Session;
9. envia cada chunk de forma sequencial;
10. repete somente falhas transitórias por um número limitado de tentativas;
11. propaga falhas finais para o Connector Hub;
12. registra logs JSON de início, HTTP, falha e fim da invocação.
```

## Principais melhorias em relação ao laboratório original

- payload multiline para reduzir a quantidade de requests ao Dynatrace;
- limite de tamanho e quantidade de linhas por request;
- pool de conexão com `requests.Session` e `HTTPAdapter`;
- timeouts separados de conexão e leitura;
- retry de `408`, `425`, `429` e `5xx` com backoff exponencial e jitter;
- uso de `Retry-After` quando presente;
- propagação de erro para permitir retry at-least-once do Connector Hub;
- tratamento de HTTP `400` parcial sem reenviar linhas já aceitas;
- suporte a API token e OAuth com cache de token por container aquecido;
- suporte a timestamps epoch e ISO-8601 na agregação;
- dimensões genéricas que preservam `resourceId`, região e compartment;
- logs de duração HTTP, CPU e RSS do processo.

## Estrutura do projeto

```text
.
├── func.py                     entrypoint, transformação e orquestração
├── dynatrace_client.py         autenticação, chunking, HTTP e retry
├── aggregation.py              timestamp e agregação por minuto
├── mint.py                     serialização do protocolo MINT
├── summary_stat.py             min/max/sum/count
├── metric_mapping.py           mappings curados; deve definir namespace_map
├── func.yaml                   runtime, memória, timeout e configurações
├── requirements.txt
└── docs/
    ├── ARCHITECTURE.md
    ├── CONFIGURATION.md
    ├── OPERATIONS.md
    ├── MAINTENANCE.md
    ├── SECURITY.md
    ├── CHANGELOG.md
    └── SOURCE_REFERENCES.md

```

## Pré-requisitos

- OCI Function Application criada na região desejada;
- subnet com rota de saída para o endpoint Dynatrace, via NAT Gateway ou proxy;
- Fn CLI configurado para a mesma região da Application;
- acesso ao OCIR para build e push da imagem;
- Connector Hub com origem OCI Monitoring e target OCI Function;
- token Dynatrace com `metrics.ingest`, ou service user OAuth autorizado para `storage:metrics:write`;
- Python 3.11, conforme o `func.yaml` atual.

## Deploy

Revise primeiro o nome da Function:

```yaml
name: oci-metric-ingestion
```

Faça o deploy no diretório que contém o `func.yaml`:

```bash
fn -v deploy --app <APPLICATION_NAME>
```

Confirme a imagem implantada:

```bash
fn inspect function <APPLICATION_NAME> <FUNCTION_NAME>
```

A URL deve conter apenas a base `https://<id>.live.dynatrace.com`. O código acrescenta `/api/v2/metrics/ingest`.

`FUNCTION_TIMEOUT_SECONDS` precisa permanecer alinhado ao timeout real, pois a implementação atual não usa `ctx.Deadline()`.

## Namespaces OCI Cache e OCI Streaming

Para métricas que não estejam mapeadas no metric_mapping.py, o uso esperado é:

```text
IMPORT_ALL_METRICS=True
```

Nesse modo, a metric key segue o padrão:

```text
cloud.oci.<namespace-sem-oci_>.<nome-da-métrica-OCI>
```

Exemplos:

```text
cloud.oci.redis.CPUUtilization
cloud.oci.streaming.PutMessagesThroughput.Count
```

## Semântica dos datapoints OCI

Um datapoint do OCI Monitoring pode conter:

```json
{
  "timestamp": 1786419199000,
  "value": 1,
  "count": 8
}
```

O campo count indica quantas ocorrências do valor estão representadas pelo
datapoint. A Function não expande essas ocorrências em memória. Ela preserva
a multiplicidade por meio de uma agregação ponderada:

```text
min   = menor value do minuto
max   = maior value do minuto
sum   = Σ(value × count)
count = Σ(count)
```

Quando count não estiver presente, a Function assume count = 1.

## Preservação dos fragmentos recebidos

A Function não deduplica linhas MINT por igualdade textual.

Duas linhas iguais podem representar dois fragmentos legítimos e independentes
entregues pelo OCI Monitoring. Remover uma dessas linhas causaria subcontagem.

Exemplo:

```text
fragmento A: sum=14,count=14
fragmento B: sum=14,count=14
```

Embora as linhas geradas sejam iguais, o total correto é:

```text
sum=28
count=28
```

A ausência de deduplicação textual não fornece exactly-once. Como o Connector
Hub trabalha com entrega at-least-once, retries entre invocações ainda podem
provocar reenvio. Eliminar esse risco exigiria idempotência persistente fora
da Function.

## Eventos de log emitidos

Cada evento é uma linha JSON em `data.message` no OCI Logging.

| Evento | Uso |
|---|---|
| `invocation_start` | registros, datapoints, bytes, RSS e idade da origem quando calculável |
| `mapping_not_found` | namespace sem mapping no modo curado |
| `metric_not_mapped` | métrica não encontrada no catálogo curado |
| `dynatrace_oauth` | obtenção/renovação do token OAuth |
| `dynatrace_http` | status, duração, chunk, tentativa, linhas e metric keys |
| `dynatrace_network_error` | connect timeout, read timeout, proxy, TLS ou conexão |
| `dynatrace_validation_error` | HTTP 400 sem detalhamento de linhas |
| `dynatrace_line_rejected` | linha rejeitada pelo Dynatrace |
| `invocation_end` | resumo de transformação, HTTP, CPU e memória |
| `invocation_failed` | falha propagada para o Connector Hub |

Consulta básica:

```text
search "<ESCOPO_DO_LOG>"
| where data.message = '*"invocation_end"*'
| sort by datetime desc
```

Use `data.opcRequestId` para correlacionar todos os registros da mesma invocação. A função auxiliar atual não consulta `ctx.CallID()`.

## Semântica de entrega

O fluxo é at-least-once:

- falhas transitórias são repetidas internamente por poucas tentativas;
- se persistirem, a Function falha e o Connector Hub pode repetir o lote;
- HTTP 400 parcial não é reenviado, porque linhas válidas podem ter sido aceitas;
- se um chunk anterior for aceito e um chunk posterior falhar, o retry do Connector Hub pode duplicar o chunk já aceito;
- não existe DLQ nem replay controlado neste desenho direto.

## Limitações conhecidas

Pontos principais:

- sem exactly-once;
- sem DLQ;
- sem validação local da janela de timestamp;
- sem probe ativo de DNS/TCP/TLS;
- sem redaction garantida de credenciais em textos de exceção;
- `metric_mapping.py` precisa ser fornecido corretamente;
- algumas opções do `func.yaml` são inativas nesta versão.

## Documentação

- [Arquitetura](docs/ARCHITECTURE.md)
- [Configuração](docs/CONFIGURATION.md)
- [Runbook operacional](docs/OPERATIONS.md)
- [Manutenção](docs/MAINTENANCE.md)
- [Segurança](docs/SECURITY.md)
- [Plano de testes](docs/TEST_PLAN.md)
- [Validação do artefato](docs/VALIDATION.md)
- [Load test](docs/LOAD_TEST.md)
- [Changelog](docs/CHANGELOG.md)
- [Referências](docs/SOURCE_REFERENCES.md)

## Suporte

Esta é uma evolução de uma receita pública, não uma extensão gerenciada ou oficialmente suportada pela Oracle ou pela Dynatrace. A equipe responsável deve manter testes, documentação, rotação de segredos, padrões corporativos e homologação a cada nova versão.
