package br.pucminas.aed.estoque.controller;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import br.pucminas.aed.estoque.domain.EventoMalformadoException;
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
 *
 * QUANDO ALGO FALHA, este metodo so lanca a excecao. Quem decide entre
 * retentar e mandar para a DLQ e o tratador de erro do container
 * (CaminhoDeFalhaConfig) — e o ack nao acontece.
 *
 * POR QUE O VALOR CHEGA COMO TEXTO
 * A desserializacao e feita aqui, e nao pelo JsonDeserializer do Kafka, por
 * dois motivos:
 *   - a DLQ recebe os BYTES ORIGINAIS do produtor, com todos os campos — e nao
 *     a nossa visao reduzida do evento, reserializada sem clienteId, valorTotal;
 *   - um JSON quebrado vira EventoMalformadoException, que o tratador de erro
 *     reconhece como permanente, em vez de uma falha de infraestrutura.
 */
@Component
public class EstoqueListener {

    private static final String CABECALHO_ID = "ce_id";

    private final EstoqueService estoqueService;
    private final ObjectMapper mapper;

    public EstoqueListener(EstoqueService estoqueService, ObjectMapper mapper) {
        this.estoqueService = estoqueService;
        this.mapper = mapper;
    }

    @KafkaListener(topics = "${demo.topico}", groupId = "estoque")
    public void aoConfirmarPedido(ConsumerRecord<String, String> registro,
                                  Acknowledgment ack) {

        String eventoId = lerCabecalho(registro, CABECALHO_ID);
        if (eventoId == null) {
            throw new EventoMalformadoException(
                    "evento sem o cabecalho " + CABECALHO_ID + "; sem ele nao ha deduplicacao");
        }

        PedidoConfirmadoEvent evento = lerEvento(registro.value());

        estoqueService.processar(eventoId, evento);

        ack.acknowledge();   // confirma DEPOIS do commit da transacao
    }

    /** Converte o JSON do fio na visao que este servico tem do evento. */
    private PedidoConfirmadoEvent lerEvento(String json) {
        if (json == null) {
            throw new EventoMalformadoException("evento sem corpo");
        }
        try {
            return mapper.readValue(json, PedidoConfirmadoEvent.class);
        } catch (JsonProcessingException e) {
            throw new EventoMalformadoException(
                    "corpo ilegivel como PedidoConfirmado: " + e.getOriginalMessage(), e);
        }
    }

    /** Le um cabecalho da mensagem e converte de bytes para texto. */
    private String lerCabecalho(ConsumerRecord<String, String> registro, String nome) {
        Header cabecalho = registro.headers().lastHeader(nome);
        if (cabecalho == null) {
            return null;
        }
        return new String(cabecalho.value(), StandardCharsets.UTF_8);
    }
}
