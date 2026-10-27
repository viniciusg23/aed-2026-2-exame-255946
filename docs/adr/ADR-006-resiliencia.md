# ADR-006 — Resiliência do servico-estoque: caminho de falha e compensação

## Título

Retentativa com limite, DLQ para a falha permanente e compensação coreografada
da reserva de estoque quando o pagamento é recusado.

## Status

**Aceito** — 26/10/2026.

## Contexto

O `servico-estoque` consome `PedidoConfirmado` (`loja.pedido.confirmado.v1`) e
reserva estoque. É idempotente por `ce_id` desde a aula 01: o registro em
`evento_processado` e o débito acontecem na mesma transação, e o offset só é
confirmado depois do commit (at-least-once).

O que faltava era o caminho de falha, e ele estava sendo decidido por omissão:

- **A falha permanente era engolida.** `EstoqueRepository.reservar()` fazia um
  `UPDATE` que, para um SKU inexistente, afetava zero linhas sem erro. O evento
  era marcado como processado e o pedido ficava sem reserva, sem que ninguém
  soubesse. Uma quantidade negativa **aumentava** o estoque.
- **A falha transitória ficava com o padrão do Spring Kafka:** 10 tentativas
  sem espera — que, com o Postgres fora do ar, se esgotam em milissegundos — e
  depois o evento era só registrado no log e descartado.
- **Não havia compensação.** No caso de exceção do Mercado Rápido (pedido
  confirmado, estoque reservado, pagamento recusado), a reserva ficava presa
  para sempre. E o estoque nem sabia o que tinha reservado para cada pedido:
  não havia como devolver.

Restrições do contexto: dois serviços, sem chamada HTTP entre eles; o contrato
é o JSON no tópico, com envelope CloudEvents em modo binário; o histórico não se
apaga — nem a linha da reserva, nem o evento do tópico.

## Decisão

### 1. Transitório × permanente

| Classe | O que é | Exemplos | Tratamento |
|---|---|---|---|
| **Permanente** | tudo que herda de `ReservaImpossivelException` | `SkuInexistenteException`, `QuantidadeInvalidaException`, `EventoMalformadoException` (JSON ilegível ou sem `ce_id`) | **não retenta**: DLQ na primeira falha |
| **Transitória** | qualquer outra exceção | `Connection refused` com o Postgres parado, timeout de conexão, broker indisponível na publicação | retenta com limite e espera; esgotado o limite, DLQ |

O critério é uma pergunta só: **tentar de novo daqui a alguns segundos pode dar
outro resultado?** Um SKU que não existe continua não existindo; um JSON
quebrado continua quebrado. Um banco que reiniciou volta.

A classificação está no tipo, e não numa lista de exceções na configuração:
`addNotRetryableExceptions(ReservaImpossivelException.class)`
([`CaminhoDeFalhaConfig.java:106`](../../servico-estoque/src/main/java/br/pucminas/aed/estoque/config/CaminhoDeFalhaConfig.java#L106)).
Um motivo permanente novo entra no caminho da DLQ só por herdar da raiz.

As exceções são lançadas **dentro** da transação de `EstoqueService.processar()`
([`EstoqueService.java:86-91`](../../servico-estoque/src/main/java/br/pucminas/aed/estoque/service/EstoqueService.java#L86-L91)).
O rollback desfaz os itens do pedido já debitados e também o registro do
`ce_id` em `evento_processado`. Sem isso, o evento reprocessado da DLQ seria
descartado como duplicata.

### 2. Quantas tentativas, com que espera, e por quê

**5 retentativas (6 tentativas ao todo), com espera exponencial: 1 s, 2 s, 4 s,
8 s, 8 s** — `ExponentialBackOffWithMaxRetries(5)`, multiplicador 2, teto de
8 s ([`application.yml`](../../servico-estoque/src/main/resources/application.yml),
bloco `estoque.retentativa`).

- **Por que ~25 s de janela.** A falha transitória que este serviço enfrenta de
  verdade é o Postgres indisponível: reinício, failover, uma janela de
  manutenção curta. Um reinício de contêiner leva poucos segundos; a janela
  cobre isso com folga. Medido no cenário 2 do README, com o Postgres parado de
  vez: 6 tentativas em **~34 s**, porque cada tentativa também espera até 2 s
  pela conexão.
- **Por que exponencial.** Muitas tentativas em sequência contra um banco que
  está voltando atrapalham a volta dele. As primeiras esperas curtas resolvem o
  soluço; as longas dão tempo à recuperação.
- **Por que não mais.** A retentativa é **bloqueante**: enquanto o evento é
  retentado, o consumidor para (ver Consequências). Mais tentativas
  significam mais tempo com o consumidor parado. Uma indisponibilidade mais longa
  que a janela não se resolve retentando em memória; ela termina na DLQ, de onde
  o evento volta pelo reprocessamento.
- **Por que `connection-timeout: 2000`.** Com o padrão do Hikari (30 s), cada
  tentativa esperaria 30 s por uma conexão; 6 tentativas passariam dos 5 min do
  `max.poll.interval.ms`, e o consumidor seria expulso do grupo no meio das
  retentativas, provocando um rebalanceamento.

**Alternativa descartada — retentativa não bloqueante (`@RetryableTopic`, com
tópicos `-retry-N`).** Ela libera o consumidor enquanto um evento espera, mas
**quebra a ordem por pedido**: um evento posterior do mesmo pedido passaria à
frente do que está esperando o retry. Como a chave é o `pedidoId` justamente
para preservar essa ordem, a retentativa bloqueante foi mantida.

### 3. A DLQ

- **Um tópico por origem:** `<tópico>.dlq` — `loja.pedido.confirmado.v1.dlq` e
  `loja.pagamento.recusado.v1.dlq`. As duas são declaradas pelo próprio
  `servico-estoque` com **3 partições**, as mesmas da origem: o
  `DeadLetterPublishingRecoverer` publica na mesma partição de onde o evento
  veio, e uma DLQ criada automaticamente pelo broker nasceria com uma só.
- **O evento chega intacto.** O recoverer copia todos os cabeçalhos originais —
  os `ce_*`, `ce_id` inclusive, que é a chave de deduplicação de quem
  reprocessar. O corpo também é o original, byte a byte: o listener recebe o
  valor como `String` e desserializa ele mesmo, em vez de usar o
  `JsonDeserializer` — que entregaria ao recoverer o objeto
  `PedidoConfirmadoEvent` deste serviço, reserializado **sem** os campos que
  ele não declara (`clienteId`, `valorTotal`, `moeda`).
- **O motivo, em cabeçalhos próprios.** O Spring acrescenta os `kafka_dlt-*`
  (`exception-fqcn`, `exception-message`, `exception-stacktrace`,
  `original-topic`, `original-partition`, `original-offset`,
  `original-timestamp`). Dois deles não servem como motivo:
  `kafka_dlt-exception-fqcn` é o `ListenerExecutionFailedException` com que o
  Spring embrulha o erro, e não a exceção de domínio; e
  `kafka_dlt-original-timestamp` é o instante em que o evento foi **produzido**,
  não o da falha. Por isso `cabecalhosDoMotivo()`
  ([`CaminhoDeFalhaConfig.java:140`](../../servico-estoque/src/main/java/br/pucminas/aed/estoque/config/CaminhoDeFalhaConfig.java#L140))
  acrescenta `dlq_excecao`, `dlq_mensagem` e `dlq_falhou_em` (ISO-8601), a
  partir da exceção de domínio encontrada na cadeia de causas.
- **O offset do evento enviado para a DLQ é confirmado:**
  `setCommitRecovered(true)`
  ([`CaminhoDeFalhaConfig.java:111`](../../servico-estoque/src/main/java/br/pucminas/aed/estoque/config/CaminhoDeFalhaConfig.java#L111)).
  O padrão é **não** confirmar. Sem isso, o evento continua "não lido" para o
  grupo, e a cada reinício do serviço é consumido de novo e mandado de novo para
  a DLQ. Isso foi descoberto rodando o cenário 1 com Docker, e não nos testes —
  ver `docs/IA.md`.

### 4. Como o evento sai da DLQ e volta ao fluxo

O `ReprocessadorDlq`
([`ReprocessadorDlq.java`](../../servico-estoque/src/main/java/br/pucminas/aed/estoque/controller/ReprocessadorDlq.java))
é um consumidor da DLQ, com grupo próprio, **desligado por padrão**
(`autoStartup = "${estoque.dlq.reprocessar:false}"`). Corrigida a causa, quem
operou a correção sobe o serviço com `--estoque.dlq.reprocessar=true`. Cada
evento volta ao tópico principal com o corpo, a chave e os cabeçalhos `ce_*`
originais, sem os `kafka_dlt-*` e `dlq_*`.

- **Idempotente pelo `ce_id`.** Reprocessar duas vezes não debita duas vezes:
  o `ce_id` original é registrado em `evento_processado` na primeira passagem,
  e a segunda é descartada. Demonstrado no cenário 1 do README e no teste A6.
- **Não cicla para sempre.** Um evento cuja causa não foi corrigida volta à
  DLQ. O cabeçalho `dlq_reprocessamentos` viaja com ele e, depois de 3 voltas, o
  reprocessador o deixa na DLQ.
- **Volta ao tópico, e não direto ao service.** Assim o evento reprocessado
  passa pelo mesmo caminho de qualquer outro — retentativa, DLQ, deduplicação —
  em vez de ter um caminho só seu.

**Em produção**, quem faria isso é o time dono do `servico-estoque`, a partir
de um alerta sobre mensagens na DLQ. Seguiria um runbook: ler `dlq_excecao` e
`dlq_mensagem`, corrigir a causa (cadastrar o SKU, corrigir o produtor, esperar
o banco), e só então reprocessar. Ligar o reprocessador é decisão de quem
corrigiu, e não algo que o serviço faz sozinho.

### 5. A compensação

- **O que foi reservado fica registrado:** tabela `reserva` (pedido, SKU,
  quantidade, `ce_id` de origem), gravada na mesma transação do débito. É só
  `INSERT`; nada nela é apagado ou alterado.
- **A recusa também é um fato registrado:** tabela `pagamento_recusado`, com
  **chave no pedido**. O `CompensacaoService.compensar()`
  ([`CompensacaoService.java:57-92`](../../servico-estoque/src/main/java/br/pucminas/aed/estoque/service/CompensacaoService.java#L57-L92)),
  numa transação só:
  1. deduplica pelo `ce_id` da recusa (linha 61) — a mesma recusa entregue duas vezes;
  2. registra a recusa com `ON CONFLICT DO NOTHING` (linha 69) — **outra** recusa,
     com outro `ce_id`, do mesmo pedido;
  3. devolve, item por item, o que a tabela `reserva` diz que foi reservado (linha 85);
  4. publica `ReservaDeEstoqueCancelada` e **espera a confirmação do broker**
     (linha 90 → [`ReservaCanceladaPublicador.java:59`](../../servico-estoque/src/main/java/br/pucminas/aed/estoque/service/ReservaCanceladaPublicador.java#L59));
  5. commit; só depois o listener confirma o offset
     ([`PagamentoRecusadoListener.java:52`](../../servico-estoque/src/main/java/br/pucminas/aed/estoque/controller/PagamentoRecusadoListener.java#L52)).
- **O `ce_id` da `ReservaDeEstoqueCancelada` é derivado do pedido**
  (`UUID.nameUUIDFromBytes("loja.estoque.reservacancelada.v1/" + pedidoId)`), e
  não sorteado. Um pedido tem no máximo uma reserva cancelada; se ela for
  publicada duas vezes, as duas cópias têm o mesmo `ce_id`.
- **Recusa sem reserva.** Quando a recusa chega antes do `PedidoConfirmado`
  (tópicos diferentes, sem ordem entre eles) ou para um pedido que nunca
  reservou (o `PedidoConfirmado` está na DLQ, por exemplo), ela é registrada com
  `reserva_devolvida = false`, nada é devolvido e **nada é publicado** — não
  houve reserva para cancelar. Se o `PedidoConfirmado` chegar depois, o
  `EstoqueService` vê a recusa
  ([`EstoqueService.java:59`](../../servico-estoque/src/main/java/br/pucminas/aed/estoque/service/EstoqueService.java#L59))
  e **não reserva**.
- **O nome é um fato:** `ReservaDeEstoqueCancelada` — particípio, linguagem do
  domínio. A compensação é um evento novo, nunca um `DELETE`.

### 6. Coreografada, e não orquestrada

O `servico-estoque` **reage** ao fato `PagamentoRecusado` e afirma outro fato,
`ReservaDeEstoqueCancelada`. Ninguém manda o estoque desfazer nada: quem simula
o pagamento só afirma que ele foi recusado, sem saber que o estoque existe.

**Alternativa descartada — orquestração**, com um orquestrador da saga que
recebe a recusa e envia ao estoque um comando "cancele a reserva do pedido X".
Ela deixaria o fluxo explícito num lugar só, mas exigiria um serviço novo, com
uma máquina de estados por pedido, e um comando — vocabulário imperativo — num
sistema cujo contrato é feito de fatos. Com **um** passo de compensação e
**um** participante, o custo não se paga. A conta muda se a saga crescer (ver
Consequências).

**Alternativa descartada — publicar depois do commit.** Se o processo cair entre
o commit e a publicação, a devolução está feita, o `ce_id` da recusa está em
`evento_processado`, e a reentrega é descartada como duplicata: o evento
`ReservaDeEstoqueCancelada` **nunca** é publicado. Publicar antes do commit
troca essa perda por uma duplicata possível, que o `ce_id` determinístico
resolve.

**Alternativa descartada, por ora — Transactional Outbox.** Gravar o evento numa
tabela na mesma transação e publicá-lo por um processo separado é a solução
robusta: elimina tanto a perda quanto a espera pelo broker dentro da
transação. Fica como o próximo passo se as consequências abaixo pesarem; para
o escopo desta atividade, a publicação síncrona com `ce_id` determinístico
resolve o problema central.

## Consequências aceitas

**Positivas**

- Nenhuma falha é mais engolida: ou o evento é processado, ou está na DLQ com o
  motivo, o instante e o corpo original.
- A DLQ é reprocessável com segurança: o mesmo `ce_id`, deduplicado.
- A reserva devolvida é exatamente a reservada, e as duas ficam no histórico.
- Nenhuma compensação se perde: o pior caso é publicada duas vezes, com o mesmo
  `ce_id`.

**Negativas — aceitas conscientemente**

1. **A retentativa bloqueia o consumidor, e com ele o tópico inteiro.** A
   espera entre as tentativas acontece na thread do consumidor. Como o serviço
   roda com uma thread por listener (o padrão, `concurrency` 1), essa thread lê
   as 3 partições de `loja.pedido.confirmado.v1`: durante até ~35 s, **nenhum**
   `PedidoConfirmado` é processado, de pedido nenhum. Com o Postgres fora do ar
   isso é coerente (nenhum conseguiria ser processado); mas uma falha transitória
   de **um** evento para todos os outros. Subir a `concurrency` para 3 reduziria
   o estrago a uma partição. O listener de `PagamentoRecusado` tem a sua própria
   thread e não é afetado.
2. **Uma indisponibilidade mais longa que a janela manda eventos válidos para a
   DLQ.** Eles não se perdem, mas dependem de alguém reprocessar. Num incidente
   longo, a DLQ enche de eventos que não tinham nada de errado.
3. **Um bug que não é de domínio é retentado à toa.** Um
   `NullPointerException`, por exemplo, não herda de `ReservaImpossivelException`
   e é retentado 6 vezes antes de ir para a DLQ: ~35 s de consumidor parado para
   um erro que nunca ia passar.
4. **A transação da compensação fica aberta esperando o broker**, até 10 s,
   com as linhas de `estoque` travadas pelo `UPDATE`. Um broker lento torna
   lenta a reserva de outros pedidos do mesmo SKU.
5. **`ReservaDeEstoqueCancelada` pode aparecer duas vezes no tópico** (publicou,
   e o commit falhou). As cópias têm o mesmo `ce_id`, mas o `ocorridoEm` difere
   — vem de `Instant.now()`. Quem consome **precisa** deduplicar pelo `ce_id`.
6. **Existe uma janela de corrida entre `PedidoConfirmado` e `PagamentoRecusado`
   do mesmo pedido.** Vêm de tópicos diferentes e são processados em threads
   diferentes. Se as duas transações rodarem ao mesmo tempo — a reserva
   verificou que não havia recusa; a recusa verificou que não havia reserva —,
   o estoque fica reservado e a recusa fica registrada como "sem reserva". Com
   `READ COMMITTED` e sem trava por pedido, nada impede isso. A janela é de
   milissegundos e exige os dois eventos do mesmo pedido praticamente juntos;
   fechar exigiria travar por pedido (`SELECT ... FOR UPDATE` numa linha do
   pedido, ou um advisory lock do Postgres).
7. **A recusa é definitiva para o pedido.** Uma vez registrada, o mesmo
   `pedidoId` nunca mais reserva. Um pagamento que fosse aprovado numa segunda
   tentativa precisaria de um pedido novo.
8. **O fluxo da compensação não está escrito num lugar só.** Para entender o que
   acontece quando um pagamento é recusado, é preciso ler o `servico-pedidos` e o
   `servico-estoque`. Com mais participantes (frete, nota fiscal), a coreografia
   fica difícil de acompanhar, e a orquestração passa a valer o custo.
9. **O estoque pode ficar negativo.** A reserva não verifica saldo. Não é
   escopo desta decisão, mas fica registrado: um `UPDATE ... WHERE quantidade >= ?`
   viraria uma terceira falha permanente, `EstoqueInsuficienteException`.

## Perguntas

### 1. O que acontece se o servico-estoque cair NO MEIO da compensação — depois de devolver a quantidade e antes de publicar ReservaDeEstoqueCancelada?

**Nada se perde e nada é devolvido duas vezes, porque nesse ponto nada foi
confirmado ainda.** Em
[`CompensacaoService.java`](../../servico-estoque/src/main/java/br/pucminas/aed/estoque/service/CompensacaoService.java),
a devolução (`repositorio.devolver(...)`, **linha 85**) e a publicação
(`publicador.publicar(cancelada)`, **linha 90**) estão dentro do mesmo método
`@Transactional` (**linha 57**). Cair entre as duas linhas é cair com a
transação **aberta**:

1. O `UPDATE estoque SET quantidade = quantidade + ?` da linha 85 não foi
   confirmado. A conexão morre com o processo, e o Postgres desfaz a transação
   inteira: a quantidade devolvida, a linha em `pagamento_recusado` e o `ce_id`
   da recusa em `evento_processado`.
2. O offset do `PagamentoRecusado` não foi confirmado: o `ack.acknowledge()` só
   acontece depois do commit
   ([`PagamentoRecusadoListener.java:52`](../../servico-estoque/src/main/java/br/pucminas/aed/estoque/controller/PagamentoRecusadoListener.java#L52)).
3. Ao voltar — ou numa outra instância, depois do rebalanceamento —, o Kafka
   entrega a mesma recusa de novo. O `ce_id` não está em `evento_processado`
   (foi desfeito), a recusa não está em `pagamento_recusado` (também foi
   desfeita), e a compensação roda do zero: devolve uma vez e publica.

Os outros dois pontos de queda do mesmo trecho:

- **Durante a publicação**, depois de o broker gravar o evento e antes de o
  `.get()` retornar
  ([`ReservaCanceladaPublicador.java:59`](../../servico-estoque/src/main/java/br/pucminas/aed/estoque/service/ReservaCanceladaPublicador.java#L59)),
  ou entre a publicação e o commit: o evento **está** no tópico, mas o banco
  desfaz a devolução. A reentrega devolve de novo — uma vez só, porque a
  primeira foi desfeita — e publica de novo, **com o mesmo `ce_id`**, porque ele
  é derivado do pedido (`idDaCompensacao`). O resultado é uma duplicata no
  tópico, que quem consome descarta (consequência negativa 5).
- **Depois do commit e antes do ack:** a reentrega encontra o `ce_id` em
  `evento_processado` e é descartada (**linha 61**). A devolução e o evento já
  aconteceram, uma vez cada.

A ordem — devolver, publicar, confirmar a transação, confirmar o offset — foi
escolhida para que **toda** queda caia num destes casos: refaz tudo, ou
descarta como duplicata. O único efeito colateral possível é um evento
duplicado com o mesmo `ce_id`, nunca um evento perdido nem uma devolução em
dobro.

### 2. Que consequência você aceitou ao escolher a sua política de retentativa?

**Aceitei parar o consumo de `PedidoConfirmado` por até ~35 s a cada evento com
falha transitória, em troca de manter a ordem por pedido.** A retentativa é
bloqueante e acontece na thread do consumidor, que neste serviço lê as 3
partições: enquanto o evento do pedido 9002 espera o Postgres voltar, todos os
outros pedidos esperam junto — inclusive os que não têm nada a ver com o
problema. A alternativa que não bloqueia, tópicos de retry,
deixaria um evento posterior do mesmo pedido passar à frente do que está
esperando, e a chave `pedidoId` existe justamente para impedir isso.

Aceitei também que a janela é **finita**. Uma indisponibilidade maior que ~35 s
manda eventos perfeitamente válidos para a DLQ, onde dependem de uma pessoa
para voltar. Escolhi isso porque uma janela maior pioraria o primeiro problema
(mais tempo de consumidor parado) e esbarraria no `max.poll.interval.ms`; e um
evento na DLQ, com o motivo e o `ce_id`, é recuperável com segurança — um
consumidor expulso do grupo no meio das retentativas, não.

E aceitei que o que **não** é de domínio é tratado como transitório por
omissão: um bug de programação é retentado 6 vezes antes de chegar à DLQ.
Preferi errar para o lado de retentar a falha que eu não conheço do que mandar
para a DLQ, na primeira tentativa, um evento que só encontrou o banco
reiniciando.
