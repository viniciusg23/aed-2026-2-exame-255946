package br.pucminas.aed.estoque.controller;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import br.pucminas.aed.estoque.domain.PedidoConfirmadoEvent;
import br.pucminas.aed.estoque.service.EstoqueService;

/**
 * O consumidor que aguenta receber duas vezes.
 *
 * A ordem aqui e deliberada e vale a discussao em sala:
 *
 *   1. processa dentro de uma transacao (no EstoqueService);
 *   2. so DEPOIS do commit e que confirma o offset.
 *
 * Se o ack acontecesse dentro da transacao, uma falha no commit apos o ack
 * perderia o evento. Confirmar depois de processar e o que caracteriza
 * at-least-once — e e justamente por isso que a idempotencia e obrigatoria.
 *
 * Este listener nao decide nada: traduz a mensagem de infraestrutura
 * (ConsumerRecord) em conceitos de dominio e delega. Toda a regra esta no
 * service.
 */
@Component
public class EstoqueListener {

    private static final String CABECALHO_ID = "ce_id";

    private final EstoqueService estoqueService;

    public EstoqueListener(EstoqueService estoqueService) {
        this.estoqueService = estoqueService;
    }

    @KafkaListener(topics = "${demo.topico}", groupId = "estoque")
    public void aoConfirmarPedido(ConsumerRecord<String, PedidoConfirmadoEvent> registro,
                                  Acknowledgment ack) {

        String eventoId = lerCabecalho(registro, CABECALHO_ID);

        estoqueService.processar(eventoId, registro.value());

        ack.acknowledge();   // confirma DEPOIS do commit da transacao
    }

    /** Le um cabecalho da mensagem e converte de bytes para texto. */
    private String lerCabecalho(ConsumerRecord<String, PedidoConfirmadoEvent> registro,
                                String nome) {
        Header cabecalho = registro.headers().lastHeader(nome);
        if (cabecalho == null) {
            return null;
        }
        return new String(cabecalho.value(), StandardCharsets.UTF_8);
    }
}
