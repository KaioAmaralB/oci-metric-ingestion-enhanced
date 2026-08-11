# Load test do OCI Streaming — 3.000 mensagens/s

Este utilitário gera e consome mensagens no OCI Streaming com o OCI Python SDK.

O alvo é **mensagens por segundo**, usando várias mensagens por request `PutMessages`. Essa distinção é importante:

```text
3.000 mensagens/s com batch 100
= aproximadamente 30 PutMessages requests/s
```

## Escopo

O script mede:

- mensagens produzidas/consumidas;
- requests PUT/GET;
- latência P50/P95 por intervalo;
- erros e throttling;
- totais ao final.

Ele não valida exatamente-once, conteúdo, sequência, ordenação ou latência ponta a ponta.

## Topologia inicial

Para mensagens de 256 bytes e 3.000 mensagens/s:

```text
Stream: 2 partições
Batch PUT: 100
GET limit: 10.000
VM conjunta: 4 OCPUs / 16 GB
```

Use a mesma região do stream.

## IAM

A VM pode usar Instance Principal.

Exemplo conceitual:

```text
Allow dynamic-group <LOAD_TEST_DYNAMIC_GROUP> to use stream-push in compartment <STREAM_COMPARTMENT>
Allow dynamic-group <LOAD_TEST_DYNAMIC_GROUP> to use stream-pull in compartment <STREAM_COMPARTMENT>
```

## Instalação

```bash
sudo dnf install -y python3 python3-pip
python3 -m venv venv
source venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements.txt
```

Configure o stream:

```bash
export OCI_STREAM_ID='ocid1.stream...'
export OCI_STREAM_ENDPOINT='https://cell-1.streaming.<region>.oci.oraclecloud.com'
```

## Execução rápida

```bash
chmod +x run-example.sh
./run-example.sh | tee "loadtest-$(date -u +%Y%m%dT%H%M%SZ).jsonl"
```

O `run-example.sh` executa:

```text
modo: both
duração: 600 s
taxa: 3.000 msg/s
tamanho: 256 bytes
batch: 100
producer workers: 4
consumer workers: 2
get limit: 10.000
```

## Execução manual — producer e consumer juntos

```bash
python3 oci_stream_load.py both \
  --duration 600 \
  --message-rate 3000 \
  --message-bytes 256 \
  --batch-size 100 \
  --producer-workers 4 \
  --consumer-workers 2 \
  --get-limit 10000 \
  --group-name loadtest-3000-001
```

## Producer-only

```bash
python3 oci_stream_load.py producer \
  --duration 600 \
  --message-rate 3000 \
  --message-bytes 256 \
  --batch-size 100 \
  --producer-workers 4
```

## Consumer-only

```bash
python3 oci_stream_load.py consumer \
  --duration 600 \
  --consumer-workers 2 \
  --get-limit 10000 \
  --group-name loadtest-3000-001 \
  --start-position trim_horizon
```

Use `trim_horizon` para consumir mensagens existentes. Um grupo novo com `latest` começa a partir do momento da criação do cursor.

## Parâmetros principais

| Parâmetro | Default | Descrição |
|---|---:|---|
| `mode` | obrigatório | `producer`, `consumer` ou `both` |
| `--duration` | `600` | duração do teste em segundos |
| `--message-rate` | `3000` | mensagens/s globais do producer |
| `--message-bytes` | `256` | bytes do valor decodificado |
| `--batch-size` | `100` | mensagens por PutMessages |
| `--producer-workers` | `4` | threads produtoras |
| `--consumer-workers` | `2` | instâncias do consumer group |
| `--get-limit` | `10000` | mensagens máximas por GET |
| `--get-rps-per-worker` | `5` | GET/s por worker |
| `--start-position` | `latest` | `latest` ou `trim_horizon` |
| `--auth` | `instance_principal` | ou `config` |

## Comportamento do producer

A taxa é dividida igualmente pelos workers:

```text
worker_rate = message_rate / producer_workers
```

O controle é best effort. Se a latência de `PutMessages` for maior que o intervalo necessário, a taxa real ficará abaixo do alvo.

As chaves são únicas para distribuir mensagens entre partições.

## Comportamento do consumer

Cada worker cria uma instância no mesmo consumer group.

O script usa:

```text
commit_on_get=True
```

Isso é adequado para teste de throughput, mas não valida commit após processamento de negócio.

Mantenha, em geral:

```text
consumer_workers <= número de partições
```

## Retry

O SDK usa `NoneRetryStrategy`.

Erros e throttling ficam visíveis e as mensagens com falha não são reenviadas pelo script.

## Output

Evento por intervalo:

```json
{
  "event": "rate",
  "produce_msg_s": 3001.2,
  "put_req_s": 30.0,
  "consume_msg_s": 2998.8,
  "get_req_s": 9.9,
  "put_p95_ms": 84.2,
  "get_p95_ms": 51.5
}
```

Resumo:

```json
{
  "event": "summary",
  "produced": 1800000,
  "producer_avg_msg_s": 3000.0,
  "consumed": 1795000,
  "consumer_avg_msg_s": 2991.7
}
```

Producer e consumer param juntos no modo `both`; uma diferença final pode representar backlog ainda não drenado.

## Métricas OCI para validar

Namespace:

```text
oci_streaming
```

Producer:

```text
PutMessagesThroughput.Count
PutMessagesSuccess.Count
PutMessagesLatency.Time
PutMessagesThrottling.Count
PutMessagesFault.Count
```

Consumer:

```text
GetMessagesThroughput.Count
GetMessagesSuccess.Count
GetMessagesLatency.Time
GetMessagesThrottling.Count
GetMessagesFault.Count
```

Com batch 100:

```text
PutMessagesThroughput.Count ~= 3.000 msg/s
PutMessagesSuccess.Count ~= 30 requests/s
```

## Monitoramento da VM

```bash
sudo dnf install -y sysstat
pidstat -p "$(pgrep -f oci_stream_load.py | head -1)" 1
sar -n DEV 1
```

Critérios iniciais:

```text
CPU < 70% sustentado
sem swap
zero throttling/faults
P95 estável
producer e consumer no alvo
```

## Limitações atuais

- sem retry de mensagem;
- sem drain automático;
- sem verificação de conteúdo/sequência;
- sem latência ponta a ponta;
- worker fatal precisa ser conferido no JSON;
- sem coordenação distribuída multi-VM.

Para plano completo, sizing, etapas de carga e correlação com Connector Hub/Function, consulte `docs/LOAD_TEST.md` no projeto principal.
