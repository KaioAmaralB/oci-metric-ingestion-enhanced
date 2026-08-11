# Referência de configuração

## 1. Regra geral

Configurações de OCI Functions chegam ao processo como strings. O projeto lê valores com `os.environ` e converte números/booleanos no início do uso.

Valores numéricos inválidos geram exceção e fazem a invocação falhar. Esse comportamento é preferível a executar com defaults silenciosos.

## 2. Variáveis utilizadas

### 2.1 Dynatrace e autenticação

| Variável | Obrigatória | Default no código | Uso atual |
|---|---:|---:|---|
| `DYNATRACE_TENANT` | sim | nenhum | URL base HTTPS do ambiente |
| `AUTH_METHOD` | não | `token` | `token` ou `oauth` |
| `DYNATRACE_API_KEY` | no modo token | vazio | API token enviado como `Api-Token` |
| `OAUTH_CLIENT_ID` | no modo OAuth | vazio | client ID do service user |
| `OAUTH_CLIENT_SECRET` | no modo OAuth | vazio | client secret |
| `OAUTH_ACCOUNT_URN` | no modo OAuth | vazio | resource/account URN |

Formato da URL SaaS:

```text
https://<environment-id>.live.dynatrace.com
```

Não inclua `/api/v2/metrics/ingest`; o código acrescenta o path.

O código rejeita hosts terminados em `.apps.dynatrace.com`.

### 2.2 Transformação

| Variável | Default no código | Uso atual |
|---|---:|---|
| `IMPORT_ALL_METRICS` | `False` | seleciona modo genérico ou catálogo curado |

Para métricas não mapeadas no metric_mapping.py, a configuração normalmente utilizada é:

```text
IMPORT_ALL_METRICS=True
```

Com `False`, `metric_mapping.py` precisa definir `namespace_map` e conter o namespace desejado.

### 2.3 Chunking HTTP

| Variável | Default no código | Valor do `func.yaml` recebido | Descrição |
|---|---:|---:|---|
| `MAX_PAYLOAD_BYTES` | `750000` | `524288` | máximo de bytes UTF-8 por request |
| `MAX_LINES_PER_REQUEST` | `5000` | `5000` | máximo de linhas por request |

O primeiro limite atingido fecha o chunk.

Recomendação inicial:

```text
MAX_PAYLOAD_BYTES=524288
MAX_LINES_PER_REQUEST=5000
```

O payload deve permanecer abaixo do limite de 1 MB da Metrics API.

### 2.4 Timeout e retry

| Variável | Default no código | Valor do YAML recebido | Descrição |
|---|---:|---:|---|
| `HTTP_CONNECT_TIMEOUT_SECONDS` | `3.0` | `3` | DNS/TCP/TLS/proxy connect |
| `HTTP_READ_TIMEOUT_SECONDS` | `10.0` | `10` | espera pela resposta após conectar |
| `HTTP_MAX_ATTEMPTS` | `2` | `3` | total de tentativas por chunk |
| `HTTP_BACKOFF_BASE_SECONDS` | `0.5` | `0.5` | base do backoff exponencial |
| `FUNCTION_SAFETY_MARGIN_SECONDS` | `3.0` | `5` | margem antes do deadline local |
| `FUNCTION_TIMEOUT_SECONDS` | `60.0` | `60` | timeout usado para o deadline local |

`FUNCTION_TIMEOUT_SECONDS` não altera a propriedade real da Function. Mantenha o mesmo valor em:

```yaml
timeout: 60
```

ou atualize ambos quando mudar o timeout.

### 2.5 Logging

| Variável | Default | Uso atual |
|---|---:|---|
| `LOG_LEVEL` | `INFO` | nível do root logger |

Valores comuns:

```text
DEBUG
INFO
WARNING
ERROR
```

### 2.6 Proxy

| Variável | Default | Uso atual |
|---|---:|---|
| `PROXY_URL` | vazio | URL do proxy para HTTP e HTTPS |
| `PROXY_USERNAME` | vazio | usuário URL-encoded |
| `PROXY_PASSWORD` | vazio | senha URL-encoded |

Exemplo:

```text
http://proxy.empresa.local:3128
```

A URL precisa conter `http://` ou `https://`.

Atenção: a implementação atual pode incluir a URL do proxy no texto de uma exceção de `requests`. Mantenha acesso ao log restrito e não armazene credenciais no Git.

### 2.7 Variável nativa da plataforma

| Variável | Origem | Uso atual |
|---|---|---|
| `FN_MEMORY` | OCI Functions | registrada em `function_memory_limit_mb` |

`FN_MEMORY` é informativa no código; ela não é usada para controlar alocação.

## 3. Dimensões genéricas efetivas

O modo genérico usa um mapeamento fixo:

| Campo OCI | Dimensão Dynatrace |
|---|---|
| valor constante | `cloud.provider=oci` |
| namespace | `oci.namespace` |
| `resourceGroup` | `oci.resource_group` |
| `compartmentId` | `oci.compartment_id` |
| `resourceId` | `oci.resource_id` |
| `region` | `oci.region` |
| `resourceDisplayName` | `oci.resource_display_name` |
| `availabilityDomain` | `oci.availability_domain` |
| `faultDomain` | `oci.fault_domain` |

Campos presentes no YAML, como `resourceName` e `resourceTenantId`, não são utilizados na versão atual.

## 5. Status HTTP e comportamento

| Status/erro | Comportamento atual |
|---|---|
| `202` | sucesso |
| `400` | registra linhas inválidas e continua |
| `408`, `425`, `429`, `500`, `502`, `503`, `504` | retry limitado; depois falha |
| outros `4xx` | falha permanente imediata |
| connect/read/proxy/TLS/connection error | retry limitado; depois falha |

Não aumente tentativas ou timeout sem analisar:

```text
http_total_ms
http_max_ms
retries
FunctionExecutionDuration
DataFreshness
```