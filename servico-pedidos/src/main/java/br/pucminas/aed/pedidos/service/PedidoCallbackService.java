package br.pucminas.aed.pedidos.service;

import java.util.function.BiConsumer;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.SendResult;

import br.pucminas.aed.pedidos.domain.PedidoConfirmadoEvent;

/**
 * O que fazer quando o broker responde sobre uma publicacao.
 *
 * Poderia ser uma lambda de duas linhas dentro do PedidoService. Virou classe
 * por dois motivos didaticos:
 *
 *   1. da nome ao conceito. O envio ao Kafka e ASSINCRONO: o metodo send()
 *      retorna imediatamente e a confirmacao chega depois, aqui;
 *   2. torna visivel o erro mais comum do produtor — ignorar esse retorno.
 *      Sem alguem escutando, uma falha de publicacao desaparece em silencio,
 *      como ignorar o retorno de um INSERT.
 *
 * O PARALELO COM O MONOLITO
 * Num sistema monolitico, tudo o que acontece aqui aconteceria dentro do
 * proprio service, logo apos o INSERT: conferir se deu certo e registrar. O
 * sufixo Service mantem esse paralelo visivel. O que mudou nao foi o QUE se
 * faz, e sim QUANDO: no monolito o resultado vem na linha seguinte; aqui ele
 * chega depois, em outra thread, e por isso precisa de um objeto esperando.
 *
 * ATENCAO A UMA DIFERENCA REAL: apesar do sufixo, esta classe nao e um bean do
 * Spring. Ela guarda o evento e por isso e criada com "new" a cada publicacao,
 * enquanto um @Service e um singleton sem estado. Nao a procure por injecao.
 *
 * Implementa BiConsumer porque e a forma que o CompletableFuture do Kafka
 * espera em whenComplete(resultado, erro): um dos dois vem preenchido.
 */
public class PedidoCallbackService
        implements BiConsumer<SendResult<String, PedidoConfirmadoEvent>, Throwable> {

    private static final Logger log = LoggerFactory.getLogger(PedidoCallbackService.class);

    private final PedidoConfirmadoEvent evento;

    public PedidoCallbackService(PedidoConfirmadoEvent evento) {
        this.evento = evento;
    }

    @Override
    public void accept(SendResult<String, PedidoConfirmadoEvent> resultado, Throwable erro) {
        if (erro != null) {
            log.error("FALHA ao publicar evento {}", evento.getEventoId(), erro);
            return;
        }
        RecordMetadata metadados = resultado.getRecordMetadata();
        log.info("publicado  evento={}  pedido={}  particao={}  offset={}",
                evento.getEventoId(),
                evento.getPedidoId(),
                Integer.valueOf(metadados.partition()),
                Long.valueOf(metadados.offset()));
    }
}
