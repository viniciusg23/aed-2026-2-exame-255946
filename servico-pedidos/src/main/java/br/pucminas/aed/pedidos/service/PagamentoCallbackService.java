package br.pucminas.aed.pedidos.service;

import java.util.function.BiConsumer;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.SendResult;

import br.pucminas.aed.pedidos.domain.PagamentoRecusadoEvent;

/**
 * O que fazer quando o broker responde sobre a publicacao de um
 * PagamentoRecusado. Mesmo papel do PedidoCallbackService: nao deixar a falha
 * de publicacao passar em silencio.
 */
public class PagamentoCallbackService
        implements BiConsumer<SendResult<String, PagamentoRecusadoEvent>, Throwable> {

    private static final Logger log = LoggerFactory.getLogger(PagamentoCallbackService.class);

    private final PagamentoRecusadoEvent evento;

    public PagamentoCallbackService(PagamentoRecusadoEvent evento) {
        this.evento = evento;
    }

    @Override
    public void accept(SendResult<String, PagamentoRecusadoEvent> resultado, Throwable erro) {
        if (erro != null) {
            log.error("FALHA ao publicar recusa {}", evento.getEventoId(), erro);
            return;
        }
        RecordMetadata metadados = resultado.getRecordMetadata();
        log.info("recusa publicada  evento={}  pedido={}  particao={}  offset={}",
                evento.getEventoId(),
                evento.getPedidoId(),
                Integer.valueOf(metadados.partition()),
                Long.valueOf(metadados.offset()));
    }
}
