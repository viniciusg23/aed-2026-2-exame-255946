# Exame especial — como reproduzir

**A falha e a volta: DLQ e compensação no Mercado Rápido.** O `servico-estoque`
passa a distinguir a falha que passa (retentativa com limite) da que não passa
(DLQ), e devolve a reserva quando o pagamento é recusado, publicando
`ReservaDeEstoqueCancelada`. As decisões estão em
[`docs/adr/ADR-006-resiliencia.md`](docs/adr/ADR-006-resiliencia.md).

| Tópico | Quem publica | Quem consome |
|---|---|---|
| `loja.pedido.confirmado.v1` | `servico-pedidos` | `servico-estoque` |
| `loja.pedido.confirmado.v1.dlq` | `servico-estoque` (falha) | `ReprocessadorDlq`, se ligado |
| `loja.pagamento.recusado.v1` | `servico-pedidos` (`POST /pagamentos/recusados`) | `servico-estoque` |
| `loja.pagamento.recusado.v1.dlq` | `servico-estoque` (falha) | ninguém |
| `loja.estoque.reservacancelada.v1` | `servico-estoque` | ninguém, por enquanto |

Os comandos abaixo rodam **da raiz do repositório** e estão em uma linha só.
No PowerShell, use `curl.exe` no lugar de `curl` (ver
["Windows: curl não é o curl"](#windows-curl-não-é-o-curl)).

## Testes automatizados

Os três cenários também são cobertos por teste, com Kafka embutido e H2 — sem
Docker:

```bash
mvn -f servico-estoque/pom.xml test
```

| Cenário | Teste |
|---|---|
| 1 — falha permanente vai para a DLQ, com `ce_id` e motivo, sem retentar | `CaminhoDeFalhaTest` A1, A2, A3, A4 |
| 1 — o offset do evento enviado para a DLQ é confirmado | `CaminhoDeFalhaTest` A4b |
| 1 — reprocessar da DLQ debita uma vez; reprocessar de novo não debita | `CaminhoDeFalhaTest` A6 |
| 2 — falha transitória é retentada e o evento é processado | `CaminhoDeFalhaTest` A5 |
| 3 — a recusa devolve o reservado e publica `ReservaDeEstoqueCancelada` | `CompensacaoTest` B1, B2 |
| 3 — a mesma recusa (ou outra do mesmo pedido) devolve uma vez só | `CompensacaoTest` B3, B4 |
| 3 — recusa sem reserva; `PedidoConfirmado` que chega depois da recusa | `CompensacaoTest` B5, B6 |

Esperado: `Tests run: 17, Failures: 0, Errors: 0` e `BUILD SUCCESS`.

## Preparação

Os passos 1 a 3 do roteiro original, mais abaixo: subir a infraestrutura,
compilar e subir os dois serviços.

```bash
docker compose up -d
mvn -f servico-pedidos/pom.xml package -DskipTests
mvn -f servico-estoque/pom.xml package -DskipTests
java -jar servico-pedidos/target/servico-pedidos-1.0.jar       # terminal 1
java -jar servico-estoque/target/servico-estoque-1.0.jar       # terminal 2
```

Para ver o estado do estoque em qualquer momento:

```bash
docker compose exec postgres psql -U aed -d aed -c "select * from estoque order by sku;"
```

Os eventos e seus cabeçalhos também podem ser vistos na Kafka UI,
<http://localhost:8081> → *Topics* → tópico → *Messages*.

> Para repetir os cenários do zero: `docker compose down -v`, `docker compose up -d`
> e suba os dois serviços de novo.

---

## Cenário 1 — falha permanente: SKU inexistente vai para a DLQ

O pedido `9001` pede o SKU `XYZ-999`, que não existe na tabela `estoque`.

```bash
curl -X POST localhost:8080/pedidos/confirmados -H "Content-Type: application/json" -d "@pedidos/pedido-9001-sku-inexistente.json"
```

Esperado no terminal do `servico-estoque` — **uma** tentativa, e direto para a
DLQ:

```
WARN  CaminhoDeFalhaConfig : falha na tentativa 1  topico=loja.pedido.confirmado.v1  particao=0  offset=0  motivo=pedido 9001 pede o SKU XYZ-999, que nao existe no estoque
ERROR CaminhoDeFalhaConfig : evento enviado para a DLQ  topico=loja.pedido.confirmado.v1  particao=0  offset=0  motivo=pedido 9001 pede o SKU XYZ-999, que nao existe no estoque
```

O evento na DLQ, com os cabeçalhos:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9094 --topic loja.pedido.confirmado.v1.dlq --from-beginning --max-messages 1 --property print.headers=true
```

Esperado — os cabeçalhos CloudEvents **originais**, `ce_id` inclusive, e o
motivo em três cabeçalhos próprios (os `kafka_dlt-*` do Spring vêm junto,
inclusive o stacktrace):

```
ce_specversion:1.0
ce_id:90010000-0000-0000-0000-000000000001
ce_source:/loja/servico-pedidos
ce_type:loja.pedido.confirmado.v1
ce_time:2026-10-26T12:00:00Z
...
dlq_excecao:br.pucminas.aed.estoque.domain.SkuInexistenteException
dlq_mensagem:pedido 9001 pede o SKU XYZ-999, que nao existe no estoque
dlq_falhou_em:2026-10-26T23:58:50.093257Z
```

seguido do corpo **original**, com todos os campos do produtor
(`"clienteId":"c-9931"`, `"valorTotal":49.90`...). O estoque não mudou
(`TEC-002 | 100`) e o `eventoId` não ficou em `evento_processado`.

### Reprocessar a partir da DLQ

Primeiro, corrigir a causa: o SKU passa a existir.

```bash
docker compose exec postgres psql -U aed -d aed -c "INSERT INTO estoque (sku, quantidade) VALUES ('XYZ-999', 10);"
```

Depois, derrubar o `servico-estoque` (Ctrl+C) e subi-lo com o reprocessador
ligado. Ele é **desligado por padrão**: reprocessar é decisão de quem operou a
correção.

```bash
java -jar servico-estoque/target/servico-estoque-1.0.jar --estoque.dlq.reprocessar=true
```

Esperado — o evento volta ao tópico principal e é processado:

```
INFO  ReprocessadorDlq : evento devolvido ao topico loja.pedido.confirmado.v1  chave=9001  reprocessamento=1
INFO  EstoqueService   : reserva aplicada  evento=90010000-0000-0000-0000-000000000001  pedido=9001
```

e `XYZ-999 | 9`. Derrube o serviço e suba-o de novo **sem** a opção.

**Reprocessar duas vezes não debita duas vezes.** Para provar, volte o grupo do
reprocessador ao início da DLQ e reprocesse de novo (com o `servico-estoque`
parado):

```bash
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9094 --group estoque-reprocessador --reset-offsets --to-earliest --topic loja.pedido.confirmado.v1.dlq --execute
java -jar servico-estoque/target/servico-estoque-1.0.jar --estoque.dlq.reprocessar=true
```

Esperado — o mesmo `ce_id` é reconhecido e descartado; o estoque continua
`XYZ-999 | 9`:

```
INFO  ReprocessadorDlq : evento devolvido ao topico loja.pedido.confirmado.v1  chave=9001  reprocessamento=1
INFO  EstoqueService   : evento 90010000-0000-0000-0000-000000000001 JA PROCESSADO, descartando em silencio
```

> Um evento cuja causa **não** foi corrigida volta para a DLQ. O cabeçalho
> `dlq_reprocessamentos` viaja com ele, e depois de 3 voltas o reprocessador
> o deixa na DLQ, em vez de ciclar para sempre.

---

## Cenário 2 — falha transitória: Postgres fora do ar

Com os dois serviços no ar (o `servico-estoque` **sem** o reprocessador):

```bash
docker compose stop postgres
curl -X POST localhost:8080/pedidos/confirmados -H "Content-Type: application/json" -d "@pedidos/pedido-9002.json"
```

O `servico-estoque` retenta, com espera crescente (1 s, 2 s, 4 s, 8 s, 8 s):

```
WARN  CaminhoDeFalhaConfig : falha na tentativa 1  topico=loja.pedido.confirmado.v1  particao=1  offset=0  motivo=Connection refused
WARN  CaminhoDeFalhaConfig : falha na tentativa 2  topico=loja.pedido.confirmado.v1  particao=1  offset=0  motivo=Connection refused
WARN  CaminhoDeFalhaConfig : falha na tentativa 3  topico=loja.pedido.confirmado.v1  particao=1  offset=0  motivo=Connection refused
```

**Em menos de ~25 segundos**, religue o Postgres:

```bash
docker compose start postgres
```

Esperado — a tentativa seguinte passa:

```
INFO  EstoqueService : reserva aplicada  evento=90020000-0000-0000-0000-000000000001  pedido=9002
```

e `TEC-002 | 99`.

> **Se o Postgres demorar mais que a janela**, as 6 tentativas se esgotam
> (~35 s) e o evento vai para a DLQ com `motivo=Connection refused`. Não se
> perde: volta pelo reprocessamento do cenário 1.

---

## Cenário 3 — compensação: pagamento recusado devolve a reserva

O pedido `9003` reserva 2 unidades do `TEC-002`:

```bash
curl -X POST localhost:8080/pedidos/confirmados -H "Content-Type: application/json" -d "@pedidos/pedido-9003.json"
docker compose exec postgres psql -U aed -d aed -c "select * from estoque where sku='TEC-002'; select pedido_id, sku, quantidade from reserva where pedido_id='9003';"
```

Esperado: `TEC-002 | 97` (partindo de 99, depois do cenário 2) e a reserva
registrada: `9003 | TEC-002 | 2`.

O pagamento é recusado:

```bash
curl -X POST localhost:8080/pagamentos/recusados -H "Content-Type: application/json" -d "@pedidos/recusa-9003.json"
```

Esperado no `servico-estoque`:

```
INFO  CompensacaoService : reserva devolvida  pedido=9003  recusa=90030000-0000-0000-0000-0000000000f1  itens=[ItemDoPedidoVO{sku=TEC-002, quantidade=2}]
```

O estoque volta, e **nada foi apagado** — a reserva continua registrada, e a
recusa fica ao lado dela:

```bash
docker compose exec postgres psql -U aed -d aed -c "select * from estoque where sku='TEC-002'; select pedido_id, sku, quantidade from reserva where pedido_id='9003'; select pedido_id, evento_id, reserva_devolvida from pagamento_recusado;"
```

```
 TEC-002 |  99
 9003    | TEC-002 | 2
 9003    | 90030000-0000-0000-0000-0000000000f1 | t
```

O evento de compensação no tópico:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9094 --topic loja.estoque.reservacancelada.v1 --from-beginning --timeout-ms 8000 --property print.headers=true --property print.key=true
```

```
ce_specversion:1.0,ce_id:87f58730-c213-3784-95ff-470736eab805,ce_source:/loja/servico-estoque,ce_type:loja.estoque.reservacancelada.v1,ce_time:2026-10-27T00:09:13.569814Z	9003	{"eventoId":"87f58730-c213-3784-95ff-470736eab805","ocorridoEm":"2026-10-27T00:09:13.569814Z","pedidoId":"9003","pagamentoRecusadoId":"90030000-0000-0000-0000-0000000000f1","itens":[{"sku":"TEC-002","quantidade":2}]}
```

O `ce_id` não é sorteado: é derivado do pedido, e por isso é sempre
`87f58730-...` para o pedido `9003`. Se a mesma compensação for publicada duas
vezes, quem consome deduplica.

**A mesma recusa entregue de novo não devolve de novo:**

```bash
curl -X POST localhost:8080/pagamentos/recusados -H "Content-Type: application/json" -d "@pedidos/recusa-9003.json"
```

```
INFO  CompensacaoService : recusa 90030000-0000-0000-0000-0000000000f1 JA PROCESSADA, descartando em silencio
```

O estoque continua `TEC-002 | 99`, e o tópico `loja.estoque.reservacancelada.v1`
continua com **um** evento.

---

# Aula 01 — Demonstração: o consumidor idempotente

**AED — Arquitetura Reativa e Event-Driven · PUC Minas / IEC**

Duas aplicações Spring Boot que conversam **apenas por eventos**. Você vai
entregar o mesmo evento três vezes e ver o estoque ser debitado **uma só vez**.
Depois vai desligar a proteção e ver a cobrança dupla acontecer.

> **Isto não é a entrega da aula 01.** A atividade avaliada é o diário de
> eventos, no enunciado do Canvas. Rodar esta demonstração serve a dois
> propósitos: entender na prática o que foi discutido em aula e, principalmente,
> **provar que o seu ambiente está pronto para a aula 02**, onde a prática
> começa sem tempo para instalação.
>
> Se algo aqui não funcionar, **abra no fórum da aula 01 durante a semana** — não
> no dia da aula 02.

---

## O que você vai observar

Três perguntas que a demonstração responde:

1. O mesmo evento chega três vezes ao tópico. Por que o estoque cai só uma vez?
2. Se eu desligar a deduplicação, o que exatamente quebra — a entrega ou o
   efeito?
3. Com o consumidor **desligado**, a API ainda aceita pedidos? O que acontece
   com eles?

---

## Pré-requisitos

| O que | Como verificar | Esperado |
|---|---|---|
| **JDK 21** | `java -version` | `openjdk version "21..."` |
| **Maven 3.9+** | `mvn -v` | versão 3.9 ou maior, com `Java version: 21` |
| **Docker** | `docker compose version` | qualquer v2.x |
| **Docker rodando** | `docker ps` | uma tabela, mesmo vazia |

**Se `java -version` disser 17 ou 11**, o projeto não vai iniciar — os `.jar`
são compilados para a versão 21 e o erro é
`UnsupportedClassVersionError: class file version 65.0`. Ajuste o `JAVA_HOME` e
o `PATH` para um JDK 21 antes de continuar.

**Se `docker ps` reclamar que não consegue conectar**, o Docker Desktop não está
em execução. Abra-o e espere ficar verde.

---

## Windows: `curl` não é o curl

No PowerShell, **`curl` é apelido de `Invoke-WebRequest`**, que não entende
`-X`, `-d` nem `@arquivo`. O comando parece certo e falha.

| Shell | O que digitar |
|---|---|
| PowerShell | **`curl.exe`** — com o `.exe` |
| Git Bash, WSL, Linux, macOS | `curl` |

Confira com `Get-Command curl`: se responder `Alias`, use `curl.exe`.

Neste README os comandos aparecem com `curl.exe`. Em Linux e macOS, tire o
`.exe` e troque a crase de quebra de linha (`` ` ``) por barra invertida (`\`).

---

## Passo 0 — descompactar

O código vem num **ZIP no Canvas**, no módulo da aula 01. Baixe e descompacte
onde preferir.

**Evite descompactar dentro de uma pasta sincronizada** — OneDrive, Google
Drive, Dropbox. A sincronização compete com o Maven pela pasta `target/` e
produz erros de arquivo em uso que parecem bug do projeto. Uma pasta simples,
como `C:\dev\aed` ou `~/dev/aed`, evita o problema.

Depois de descompactar, entre na pasta:

```bash
cd demo-kafka-idempotencia
```

**Todos os comandos deste README rodam a partir desta pasta**, a raiz do
projeto — nunca de dentro de `servico-pedidos/`. Os caminhos dos `.jar` e dos
arquivos de pedido são relativos a ela. Três erros diferentes vêm de estar na
pasta errada, e nenhum deles diz "pasta errada":

| Comando | Mensagem enganosa |
|---|---|
| `java -jar servico-pedidos/target/...` | `Unable to access jarfile` |
| `curl.exe ... -d "@pedidos/pedido-4711.json"` | falha ao ler o arquivo |
| `docker compose down` | `no configuration file provided: not found` |

---

## Passo 1 — subir a infraestrutura

```bash
docker compose up -d
docker compose ps
```

Sobem três contêineres: **Kafka**, **Postgres** e a **Kafka UI**. O comando
demora alguns segundos e só retorna quando o Kafka está pronto — isso é
proposital.

Esperado:

```
NAME           STATUS
aed-kafka      Up (healthy)
aed-kafka-ui   Up
aed-postgres   Up (healthy)
```

> As portas são **19092** (Kafka) e **15432** (Postgres), e não as padrão. Muita
> gente já tem Kafka ou Postgres na 9092 e na 5432; quando isso acontece, a
> aplicação conecta no serviço errado e o erro que aparece é de **autenticação**,
> que manda você procurar no lugar errado.

---

## Passo 2 — compilar

```bash
mvn -f servico-pedidos/pom.xml package -DskipTests
mvn -f servico-estoque/pom.xml package -DskipTests
```

A primeira execução baixa dependências e demora. **Faça isso antes da aula 02**,
com internet boa.

Confira que os dois `.jar` existem:

```bash
ls servico-*/target/*.jar          # Linux, macOS, Git Bash
dir servico-*\target\*.jar         # PowerShell
```

Sem este passo, o próximo comando responde `Unable to access jarfile`: a pasta
`target/` só existe depois do `package`.

---

## Passo 3 — subir os dois serviços

Em **dois terminais separados**, para poder derrubar um sem o outro.

Terminal 1 — **suba este primeiro**, é ele que cria o tópico:

```bash
java -jar servico-pedidos/target/servico-pedidos-1.0.jar
```

Terminal 2:

```bash
java -jar servico-estoque/target/servico-estoque-1.0.jar
```

Deixe **os dois visíveis**: os logs são metade da demonstração.

Esperado, no fim de cada um: `Started PedidoApplication` e
`Started EstoqueApplication`.

> Se você subir o consumidor primeiro, ele reclama de `UNKNOWN_TOPIC_OR_PARTITION`
> até o tópico existir. Não é erro fatal, mas atrasa.

### Como observar o estado

O `servico-estoque` **não expõe HTTP de propósito** — um consumidor não precisa
de porta. O estado se observa no banco:

```bash
docker compose exec postgres psql -U aed -d aed -c "select * from estoque; select count(*) from evento_processado;"
```

Esperado agora: `TEC-002 | 100` e `count = 0`.

---

## Passo 4 — publicar um pedido

O evento **não é inventado pela aplicação**: ele vai no corpo da requisição e é
desserializado direto na classe de domínio. Não há fábrica nem endpoint que gere
pedido. Os payloads prontos estão em `pedidos/`.

Abra `pedidos/pedido-4711.json` antes de enviar. Repare no `eventoId` **fixo** —
é ele que faz a repetição ser o *mesmo* evento:

```json
{ "eventoId":   "11111111-1111-1111-1111-111111111111",
  "ocorridoEm": "2026-08-03T22:14:07.512Z",
  "pedidoId":   "4711",
  "clienteId":  "c-9931",
  "valorTotal": 289.90,
  "moeda":      "BRL",
  "itens": [ { "sku": "TEC-002", "quantidade": 1, "preco": 289.90 } ] }
```

```powershell
curl.exe -s -w "`nHTTP %{http_code}`n" -X POST localhost:8080/pedidos/confirmados `
  -H "Content-Type: application/json" -d "@pedidos/pedido-4711.json"
```

Esperado: **HTTP 202**.

> **Pare no 202, não é 200.** A API não confirmou a reserva — confirmou que
> *aceitou* o pedido. No instante da resposta o estoque ainda não mudou, e o
> serviço que muda pode nem estar rodando. É o passo 7 que torna isso visível.

Nos logs:

```
[pedidos] publicado  evento=11111111-...  pedido=4711  particao=1  offset=0
[estoque] reserva aplicada  evento=11111111-...  pedido=4711
```

Confira o banco: **estoque 100 → 99**, `evento_processado` com 1 linha.

---

## Passo 5 — repetir o MESMO comando (o ponto da aula)

Seta para cima e Enter. **Duas vezes.** É literalmente o mesmo evento, porque o
`eventoId` está no arquivo.

```
[pedidos] publicado  evento=11111111-...  particao=1  offset=1
[estoque] evento 11111111-... JA PROCESSADO, descartando em silencio
[pedidos] publicado  evento=11111111-...  particao=1  offset=2
[estoque] evento 11111111-... JA PROCESSADO, descartando em silencio
```

O estoque **permanece 99**.

> **Pense antes de olhar o banco:** o evento chegou ao consumidor, e o `offset`
> avançou de 0 para 2 — são três registros distintos no tópico. Por que o
> estoque não caiu três vezes?
>
> Porque **chegar ao tópico e ter efeito são coisas diferentes**. O broker
> entregou as três vezes, corretamente. Quem decidiu não repetir o efeito foi o
> consumidor, consultando a tabela `evento_processado` pela chave do evento.

---

## Passo 6 — desligar a deduplicação

Não há endpoint para isso. Derrube o consumidor e suba com uma propriedade:

```bash
# Ctrl+C no terminal 2, depois:
java -jar servico-estoque/target/servico-estoque-1.0.jar --demo.deduplicacao-ativa=false
```

Republique o mesmo pedido do passo 4:

```
[estoque] deduplicacao DESLIGADA - processando 11111111-... sem verificar
[estoque] reserva aplicada  evento=11111111-...  pedido=4711
```

Estoque: **99 → 98**. É a cobrança dupla.

> Repare no que **não** mudou: o Kafka, o produtor, o contrato do evento, a
> entrega. Tudo continuou correto. O que quebrou foi o **efeito de negócio** —
> e a responsabilidade era do consumidor.

Suba de novo **sem** a propriedade e republique: o estoque não se move mais.

**Use `Ctrl+C`, nunca matar o processo à força.** Matando à força, o coordenador
do grupo não sabe que o consumidor saiu e espera o `session.timeout` expirar:
são uns 30 segundos de tela parada.

---

## Passo 7 — o desacoplamento temporal

O momento mais forte da demonstração.

```bash
# Ctrl+C no terminal 2 — o consumidor SAI DO AR
```

Com o consumidor morto, publique **quatro pedidos diferentes**. Use
`pedidos/pedido-4712.json` e crie outros copiando um arquivo e trocando
**`eventoId` e `pedidoId`** — trocar só o `pedidoId` não adianta, porque a
deduplicação olha o `eventoId`.

Todos respondem **202**. Olhe o banco: **o estoque não mudou.**

Agora suba o consumidor de novo. Ele processa tudo o que perdeu, de uma vez.

> A API aceitou pedidos com o serviço de estoque **morto**, e nada se perdeu.
> Isso é desacoplamento temporal demonstrado, não afirmado. Numa chamada HTTP
> síncrona entre os dois serviços, esses quatro pedidos teriam falhado.

---

## Kafka UI — <http://localhost:8081>

Sobe junto com o `docker compose up -d`. Sem login. O cluster chama-se `aed`.

| Onde | O que ver |
|---|---|
| **Topics** | `loja.pedido.confirmado.v1`, com 3 partições |
| Topics → Messages | cada evento: chave, corpo, partição, offset |
| Messages → uma mensagem → **Headers** | `ce_specversion`, `ce_id`, `ce_source`, `ce_type`, `ce_time` |
| **Consumers** | o grupo `estoque` e o **lag** por partição |

Vale abrir os **cabeçalhos** de uma mensagem: é o envelope CloudEvents em modo
binário, com os atributos fora do corpo. O consumidor consegue deduplicar pelo
`ce_id` sem sequer desserializar a carga.

No passo 7, acompanhe o **lag** subindo com o consumidor desligado, e caindo a
zero quando ele volta.

---

## Recomeçar do zero

Voltar o estoque a 100 e limpar a memória de deduplicação:

```bash
docker compose exec postgres psql -U aed -d aed -c "UPDATE estoque SET quantidade=100; DELETE FROM evento_processado;"
```

Isso limpa o **banco**, não o tópico: os eventos publicados continuam no Kafka.

Para apagar tudo, inclusive o tópico e os volumes:

```bash
docker compose down -v
```

---

## Rodar os testes (sem Docker)

```bash
mvn -f servico-estoque/pom.xml test
```

Quatro testes com Kafka embutido e H2 — **sem Docker e sem o `servico-pedidos`**.
Poder testar o consumidor sem subir o produtor é justamente o que a separação em
duas aplicações permite: os dois lados só se conhecem pelo tópico.

```
1 - evento novo reserva estoque uma vez
2 - o MESMO evento entregue tres vezes reserva uma so vez
3 - sem deduplicacao, a mesma entrega debita o estoque de novo
4 - campos que o consumidor nao declara sao ignorados

Tests run: 4, Failures: 0, Errors: 0
```

Se este comando passar, seu ambiente Java e Maven está correto — falta só o
Docker.

---

## Problemas comuns

| Sintoma | Causa provável | O que fazer |
|---|---|---|
| `UnsupportedClassVersionError ... 65.0` | Java menor que 21 | conferir `java -version`; ajustar `JAVA_HOME` e `PATH` |
| `Unable to access jarfile` | faltou o `mvn package`, ou você está na pasta errada | rodar o passo 2, a partir da raiz do projeto |
| `no configuration file provided: not found` | `docker compose` fora da pasta do projeto | `cd` para a raiz, ou usar `-f caminho/docker-compose.yml` |
| `curl` reclama de parâmetro no PowerShell | apelido de `Invoke-WebRequest` | usar `curl.exe` |
| `password authentication failed` | outro Postgres na sua máquina atendendo na porta | conferir se o `docker compose ps` mostra `aed-postgres`; portas aqui são 15432 e 19092 |
| `Connection refused` na porta 8080 | `servico-pedidos` não subiu | ver o terminal 1 |
| Consumidor não processa nada | subiu antes do tópico existir | esperar alguns segundos, ou reiniciá-lo |
| Estoque não volta a 100 ao reiniciar | é intencional | usar o comando de recomeçar do zero |
| Eventos somem, metade não é processada | há **mais de um** `servico-estoque` rodando | encerrar os extras: as partições se dividem entre eles |

Se nada disso resolver, poste no **fórum da aula 01** com: o comando que rodou,
a mensagem completa, e a saída de `java -version` e `docker compose ps`.

---

## Onde olhar no código

O projeto é deliberadamente **verboso e orientado a objetos**: sem lambdas ou
streams onde uma classe nomeada resolve. A intenção é que haja o que abrir no
depurador e o que apontar em aula.

### `servico-pedidos` — API REST e produtor (porta 8080, sem banco)

| Arquivo | O que ensina |
|---|---|
| `domain/PedidoConfirmadoEvent.java` | evento imutável explícito: `private final`, cópia defensiva, sem setter |
| `domain/ItemDoPedidoVO.java` | value object: igualdade por valor, sem identidade |
| `controller/PedidoController.java` | `@RequestBody` e **202 Accepted** |
| `service/PedidoService.java` | chave de partição e o envelope CloudEvents nos cabeçalhos |
| `service/PedidoCallbackService.java` | o retorno de `send()` tem dono; ignorá-lo esconde a falha |
| `PedidoConfig.java` | criação do tópico com 3 partições |

### `servico-estoque` — consumidor e banco (sem servidor web)

| Arquivo | O que ensina |
|---|---|
| `domain/PedidoConfirmadoEvent.java` | **declara menos campos que o produtor** — consumidor tolerante |
| `controller/EstoqueListener.java` | o `ack` vem **depois** do commit: é o que caracteriza at-least-once |
| `service/EstoqueService.java` | `@Transactional`: deduplicação e efeito de negócio no **mesmo commit** |
| `service/EstoqueRepository.java` | as duas tabelas: memória do que já foi visto, e o efeito |
| `resources/schema.sql` | a tabela `evento_processado` e a nota sobre retenção da chave |

> Os dois projetos são Maven **independentes**: sem POM pai, sem módulo de
> contrato compartilhado. Cada lado declara a **própria** classe do evento — e o
> consumidor declara menos campos que o produtor, de propósito. O contrato entre
> eles é o JSON que trafega no tópico, e mais nada. Compartilhar um JAR recriaria
> o acoplamento de compilação que os eventos deveriam eliminar.
