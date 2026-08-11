# Changelog

## Documentação revisada — 2026-08-10

## Versão `0.0.1`

### `func.py`

- transforma todo o lote antes do envio HTTP;
- suporta modo genérico e modo curado;
- agrega datapoints por minuto;
- gera múltiplas linhas MINT;
- remove duplicatas exatas dentro da invocação;
- preserva dimensões genéricas fixas, incluindo `resourceId` e região;
- reutiliza `DynatraceClient` por container aquecido;
- calcula deadline local por `FUNCTION_TIMEOUT_SECONDS`;
- registra início, fim e falha da invocação;
- registra RSS atual/pico e CPU total do processo;
- propaga exceções finais ao Connector Hub.

### `dynatrace_client.py`

- payload multiline;
- chunking por bytes e linhas;
- `requests.Session` com pool;
- autenticação API token e OAuth;
- cache de token OAuth com renovação antecipada;
- timeouts separados de conexão e leitura;
- retry limitado para status e erros transitórios;
- backoff exponencial com jitter e `Retry-After`;
- validação de HTTP 202;
- tratamento de HTTP 400 parcial;
- classificação de erros de rede;
- logs estruturados por chunk/tentativa.

### `aggregation.py`

- suporte a epoch segundos, epoch milissegundos e ISO-8601;
- validação de timestamp e valor;
- agregação por minuto UTC;
- funções `max`, `min`, `sum` e `mean` compatíveis com mappings.

### `mint.py`

- validação básica da metric key;
- dimensões em minúsculas e ordenadas;
- remoção de dimensões vazias;
- escape de valores;
- serialização gauge.
