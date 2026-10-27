# Uso de IA

**Ferramenta:** Claude Code (Anthropic), no terminal, com acesso ao repositório.
Usada para ler o enunciado, planejar, escrever código, testes e documentação, e
rodar os testes e os cenários com Docker. Todo commit foi revisado e feito em meu
nome; as decisões abaixo são minhas.

## Recusas

### 1. Recusei considerar a Parte A pronta só com os testes automatizados verdes

**O que a ferramenta afirmou:** que, com `ack-mode: manual_immediate`, o
`DefaultErrorHandler` confirmaria sozinho o offset de um evento enviado para a
DLQ. Chegou a escrever isso num comentário do `application.yml`. Os 10 testes
passavam.

**Por que recusei:** os testes rodam com Kafka embutido e **nunca reiniciam o
consumidor** — e o que acontece com o offset só aparece depois de um reinício.
Exigi rodar os três cenários do README com Docker antes de seguir. No cenário 1,
ao reiniciar o `servico-estoque` com o reprocessador, o consumidor principal
**releu** o evento que já estava na DLQ. O `kafka-consumer-groups --describe`
confirmou: na partição 2, quatro eventos já enviados para a DLQ e nenhum offset
confirmado. A cada reinício, cada evento da DLQ seria consumido e mandado para a
DLQ de novo.

**O que ficou:** o padrão do spring-kafka 3.2.4 é **não** confirmar o offset de
um evento recuperado. A correção é `tratador.setCommitRecovered(true)` no
`CaminhoDeFalhaConfig`. Antes de corrigir, escrevi o teste
`CaminhoDeFalhaTest` A4b, que lê o offset confirmado do grupo pela Admin API, e o
vi falhar; com a correção, passou. Commit
`estoque: confirma o offset do evento enviado para a DLQ`.

### 2. Recusei seguir para a Parte B sem compilar e testar a Parte A

**O que a ferramenta fez:** não encontrou o Maven no `PATH` e escreveu a Parte A
inteira sem compilar. Avisou que nada tinha sido testado, mas os três commits da
Parte A foram criados assim mesmo, e a sugestão seguinte já era começar a
Parte B.

**Por que recusei:** a Parte B mexe nos mesmos arquivos (`EstoqueService`,
`EstoqueRepository`, `application.yml`). Construir em cima de código que nunca
compilou empilharia erro sobre erro, e corrigir depois significaria reescrever
commits. O Maven estava instalado (pelo SDKMAN), só fora do `PATH`. Os testes
rodaram e passaram antes de qualquer linha da Parte B.


### 3. Chave `(pedido_id, sku)` na tabela `reserva`

**Sugestão inicial:** chave primária `(pedido_id, sku)` na tabela `reserva`, e
uma `PedidoJaReservadoException` (permanente, para a DLQ) quando chegasse um
segundo `PedidoConfirmado` do mesmo pedido.

**Motivo:** o teste 3 do `IdempotenciaTest` desliga a deduplicação de propósito
para mostrar o débito em dobro — é a demonstração da aula 01. Com a chave, o
segundo débito viraria `PedidoJaReservadoException` e iria para a DLQ: o teste
quebraria, e a demonstração deixaria de mostrar o incidente que ela existe para
mostrar.

**O que ficou:** chave própria (`id`), uma linha por item debitado, e
`reservasDoPedido()` soma por SKU. A compensação devolve exatamente o que foi
debitado, inclusive no modo de demonstração. Commit
`estoque: registra o que reservou por pedido`.

### 4. O motivo da falha só nos cabeçalhos `kafka_dlt-*`

**Sugestão inicial:** confiar nos cabeçalhos que o `DeadLetterPublishingRecoverer`
já acrescenta (`kafka_dlt-exception-fqcn`, `kafka_dlt-exception-message`) como
"o motivo" exigido pelo enunciado, e acrescentar só o instante da falha.

**Motivo:** o Spring embrulha o erro do listener num
`ListenerExecutionFailedException`. É ele que aparece em
`kafka_dlt-exception-fqcn`, e a mensagem dele não traz a do domínio. Quem
abrisse a DLQ leria "o listener lançou uma exceção", e não "o pedido 9001 pede
o SKU XYZ-999, que não existe". O motivo de verdade só estaria enterrado no
stacktrace.

**O que ficou:** `cabecalhosDoMotivo()` percorre a cadeia de causas até a
`ReservaImpossivelException` (ou, se não houver uma, a causa mais profunda) e
grava `dlq_excecao` e `dlq_mensagem`, além de `dlq_falhou_em`. Conferido depois,
no evento real da DLQ no Docker: `kafka_dlt-exception-fqcn` é mesmo o
`ListenerExecutionFailedException`, e o `dlq_excecao` é o
`SkuInexistenteException`.

### 5. "Partição" no lugar de "consumidor", no ADR-006

**Sugestão inicial:** descrever a consequência da retentativa bloqueante como
"a partição do evento para enquanto ele é retentado".

**Motivo:** o serviço roda com `concurrency` 1, o padrão. Uma única thread lê
as três partições de `loja.pedido.confirmado.v1`, e é nela que acontece a espera
entre as tentativas: o que para é o consumo de `PedidoConfirmado` **inteiro**,
de todos os pedidos. Escrever "partição" subestimava a principal consequência
negativa da política de retentativa — justamente a da pergunta 2 do ADR.

**O que ficou:** o ADR fala em consumidor parado, diz que subir a `concurrency`
para 3 reduziria o estrago a uma partição, e registra que o listener de
`PagamentoRecusado` tem thread própria e não é afetado.

## Aceites

Sugestões da ferramenta que aceitei, com o motivo.

| #  | Sugestão                                                                                                               | Por que aceitei                                                                                                                                                                                                                                                                                   |
|----|------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 6  | **Hierarquia de exceções** (`ReservaImpossivelException` como raiz). A ferramenta ofereceu também uma exceção única.   | O "não retentar" fica declarado num lugar só (`addNotRetryableExceptions` da raiz). Um motivo permanente novo entra no caminho da DLQ só por herdar dela.                                                                                                                                         |
| 7  | **Consumir o valor como `String`** e desserializar no listener, em vez do `JsonDeserializer`.                          | Com o `JsonDeserializer`, a DLQ receberia o objeto deste serviço reserializado, sem `clienteId`, `valorTotal` e `moeda`. Quem reprocessa precisa do evento original. De quebra, JSON ilegível vira `EventoMalformadoException`, permanente.                                                       |
| 8  | **5 retentativas com espera exponencial**, de 1 s a 8 s.                                                               | Cobre um reinício do Postgres com folga e não martela o banco enquanto ele volta. Medido no Docker: ~34 s até a DLQ. Mais que isso seria mais tempo com o consumidor parado.                                                                                                                      |
| 9  | **`hikari.connection-timeout: 2000`**.                                                                                 | Com o padrão de 30 s por tentativa, seis tentativas passariam dos 5 min do `max.poll.interval.ms`, e o consumidor seria expulso do grupo no meio das retentativas.                                                                                                                                |
| 10 | **Reprocessador como consumidor da DLQ, desligado por padrão**, com limite de 3 voltas.                                | Reprocessar é decisão de quem corrigiu a causa, não do serviço. O evento volta ao tópico e passa pelo mesmo caminho de todos. O limite impede o ciclo DLQ → tópico → DLQ de um evento cuja causa não foi corrigida.                                                                               |
| 11 | **Cabeçalhos próprios `dlq_excecao`, `dlq_mensagem` e `dlq_falhou_em`**, que substituiu a sugestão recusada no item 4. | O `kafka_dlt-exception-fqcn` traz o `ListenerExecutionFailedException`, não a exceção de domínio, e o `kafka_dlt-original-timestamp` é o instante da produção, não o da falha. As duas coisas foram conferidas no evento real da DLQ, no Docker.                                                  |
| 12 | **Retentativa bloqueante, e não `@RetryableTopic`**.                                                                   | Tópicos de retry deixariam um evento posterior do mesmo pedido passar à frente do que espera. A chave `pedidoId` existe para manter essa ordem.                                                                                                                                                   |
| 13 | **Publicar `ReservaDeEstoqueCancelada` dentro da transação, antes do commit**, com `ce_id` derivado do pedido.         | Publicar depois do commit pode perder o evento para sempre, porque a reentrega da recusa seria descartada como duplicata. Antes do commit, o pior caso é publicar duas vezes, e o `ce_id` determinístico deixa o consumidor descartar a cópia. Outbox ficou registrado no ADR como próximo passo. |
| 14 | **Recusa sem reserva:** fica registrada, nada é publicado, e um `PedidoConfirmado` que chega depois não reserva.       | Os dois eventos vêm de tópicos diferentes, sem ordem entre eles. Ignorar a recusa deixaria o estoque preso quando o pedido chegasse. Publicar `ReservaDeEstoqueCancelada` afirmaria um fato que não aconteceu.                                                                                    |
| 15 | **Endpoint `POST /pagamentos/recusados`** no `servico-pedidos`, em vez de um script publicando no tópico.              | Reaproveita o envelope CloudEvents que o serviço já monta e deixa o cenário 3 do README com o mesmo comando dos outros (`curl`).                                                                                                                                                                  |