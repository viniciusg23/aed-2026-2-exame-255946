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
import br.pucminas.aed.estoque.domain.PagamentoRecusadoEvent;
import br.pucminas.aed.estoque.service.CompensacaoService;

/**
 * Consome PagamentoRecusado e delega a compensacao.
 *
 * Mesmo desenho do EstoqueListener: o ack so acontece depois do commit da
 * transacao do CompensacaoService, e qualquer falha sobe para o tratador de
 * erro do container — que retenta o transitorio e manda o permanente para
 * loja.pagamento.recusado.v1.dlq.
 */
@Component
public class PagamentoRecusadoListener {

    private static final String CABECALHO_ID = "ce_id";

    private final CompensacaoService compensacaoService;
    private final ObjectMapper mapper;

    public PagamentoRecusadoListener(CompensacaoService compensacaoService, ObjectMapper mapper) {
        this.compensacaoService = compensacaoService;
        this.mapper = mapper;
    }

    @KafkaListener(topics = "${estoque.topico-pagamento-recusado}", groupId = "estoque")
    public void aoRecusarPagamento(ConsumerRecord<String, String> registro, Acknowledgment ack) {

        String eventoId = lerCabecalho(registro, CABECALHO_ID);
        if (eventoId == null) {
            throw new EventoMalformadoException(
                    "recusa sem o cabecalho " + CABECALHO_ID + "; sem ele nao ha deduplicacao");
        }

        PagamentoRecusadoEvent recusa = lerEvento(registro.value());

        compensacaoService.compensar(eventoId, recusa);

        ack.acknowledge();   // confirma DEPOIS do commit da transacao
    }

    private PagamentoRecusadoEvent lerEvento(String json) {
        if (json == null) {
            throw new EventoMalformadoException("recusa sem corpo");
        }
        try {
            return mapper.readValue(json, PagamentoRecusadoEvent.class);
        } catch (JsonProcessingException e) {
            throw new EventoMalformadoException(
                    "corpo ilegivel como PagamentoRecusado: " + e.getOriginalMessage(), e);
        }
    }

    private String lerCabecalho(ConsumerRecord<String, String> registro, String nome) {
        Header cabecalho = registro.headers().lastHeader(nome);
        if (cabecalho == null) {
            return null;
        }
        return new String(cabecalho.value(), StandardCharsets.UTF_8);
    }
}
