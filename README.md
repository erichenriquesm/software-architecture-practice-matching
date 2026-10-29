# software-architecture-practice-matching

Serviço `matching` da [plataforma de trading](https://github.com/erichenriquesm/software-architecture-practice) (projeto de prática de arquitetura de software).

Matching: o motor do livro de ofertas. Prioridade preço-tempo, execução parcial em cascata e prevenção de self-trade. Fonte da verdade das negociações.

## O que faz

Mantém o livro de ofertas de cada mercado: a melhor compra casa com a melhor venda, a quantidade é a menor das duas, o preço é o da ordem mais antiga e o lado é o da ordem que chegou. Uma conta nunca negocia com ela mesma. Processa as ordens de um mercado uma de cada vez, na ordem de chegada.

A especificação completa (modelo, casos de uso, dados, testes) está na spec do serviço: [`sdd/services/matching.md`](https://github.com/erichenriquesm/software-architecture-practice/blob/main/sdd/services/matching.md). As regras de arquitetura valem para todos os serviços: [`sdd/01-principios.md`](https://github.com/erichenriquesm/software-architecture-practice/blob/main/sdd/01-principios.md).

## Comece por aqui

Uma ordem casando, de ponta a ponta:

1. `src/infra/messaging/consumers/marketOrdersConsumer.ts`: recebe `order.accepted` da fila do mercado, uma mensagem por vez.
2. `src/application/usecases/executeOrder.ts`: carrega o livro, coloca a ordem e salva.
3. `src/domain/entities/orderBook.ts`: **a regra central**: prioridade preço-tempo, execução parcial em cascata, prevenção de self-trade. Comece a leitura do domínio por aqui.
4. `src/infra/database/kyselyOrderBookRepository.ts`: grava o livro e os trades no Postgres.
5. `src/application/usecases/bookStore.ts` entrega os eventos do livro ao `EventPublisher`, na mesma transação.
6. `src/infra/messaging/outboxEventPublisher.ts`: transforma os eventos em `matching.tradeExecuted`, `matching.bookSnapshot` etc. e grava no outbox; o worker publica.

## API

Não tem API pública: só o `/health` do worker.

**gRPC interno:** Não tem.

## Eventos (RabbitMQ)

- **Publica:** `matching.tradeExecuted.<mercado>`, `matching.orderRemoved.<mercado>`, `matching.orderCancelRejected.<mercado>`, `matching.bookSnapshot.<mercado>`
- **Consome:** `order.accepted`, `order.cancelRequested` (uma fila `matching.orders.<mercado>` por mercado, um consumidor ativo por vez)

Os contratos ficam em `src/infra/schemas/`: `published/` (este serviço é o dono) e `consumed/` (cópias do que ele consome, conferidas pelo `make check` do repositório guarda-chuva).

## Como rodar

Este serviço roda junto com a plataforma, a partir do [repositório guarda-chuva](https://github.com/erichenriquesm/software-architecture-practice), que tem a infra compartilhada (RabbitMQ, Redis, edge) e clona todos os serviços:

```bash
git clone https://github.com/erichenriquesm/software-architecture-practice.git
cd software-architecture-practice
make clone
make up
```

Containers: `matching-worker` (único processo; se perder a conexão com o RabbitMQ ele sai e o Docker o reinicia) · `matching-db`. Portas no host (só para depurar): health 3004 · Postgres 5436.

Dentro da pasta do serviço (`backend/matching/`):

| Comando | O que faz |
|---|---|
| `make up` · `make down` | sobe só este serviço (com RabbitMQ e Redis, sem os outros serviços nem o edge) e roda as migrações · derruba os containers dele |
| `make watch` · `make ps` · `make logs` | o mesmo `up` em primeiro plano com Compose Watch · containers e saúde · logs |
| `make test` | typecheck, lint, regras de arquitetura, testes de unidade e de integração (tudo em container) |
| `make migrate` · `make migrate-status` · `make migrate-rollback` · `make migration name=...` | migrações no estilo Laravel |
| `make deps` | instala o `node_modules` na sua máquina, só para o editor |

## Estrutura

```
src/
  domain/        regras de negócio (entities, valueObjects, events)
  application/   casos de uso (com seus Input) e contratos (interfaces)
  infra/         HTTP, mensageria, banco, plataforma técnica, schemas de eventos
test/
  unit/  integration/  fakes/  support/
docker/          Dockerfile e composes (só para ambiente local)
```
