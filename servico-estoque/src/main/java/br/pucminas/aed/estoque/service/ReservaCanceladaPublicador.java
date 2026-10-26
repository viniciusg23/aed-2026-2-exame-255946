package br.pucminas.aed.estoque.service;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import br.pucminas.aed.estoque.domain.ReservaDeEstoqueCanceladaEvent;

/**
 * Publica ReservaDeEstoqueCancelada, com envelope CloudEvents em modo binario
 * — os mesmos atributos que o PedidoConfirmado usa.
 *
 * O ENVIO E SINCRONO, e isso e deliberado: o CompensacaoService chama este
 * metodo DENTRO da transacao, antes do commit. Se o broker nao confirmar, a
 * excecao sobe, a transacao faz rollback (a quantidade devolvida volta a ser
 * reservada) e o PagamentoRecusado e retentado. Ver ADR-006, pergunta 1.
 */
@Component
public class ReservaCanceladaPublicador {

    private static final String TIPO_DO_EVENTO = "loja.estoque.reservacancelada.v1";
    private static final String ORIGEM = "/loja/servico-estoque";
    private static final String VERSAO_CLOUDEVENTS = "1.0";
    private static final long PRAZO_DO_ENVIO_SEGUNDOS = 10;

    private final KafkaTemplate<String, String> template;
    private final ObjectMapper mapper;
    private final String topico;

    public ReservaCanceladaPublicador(KafkaTemplate<String, String> template,
                                      ObjectMapper mapper,
                                      @Value("${estoque.topico-reserva-cancelada}") String topico) {
        this.template = template;
        this.mapper = mapper;
        this.topico = topico;
    }

    public void publicar(ReservaDeEstoqueCanceladaEvent evento) {
        ProducerRecord<String, String> registro =
                new ProducerRecord<String, String>(topico, evento.getPedidoId(), paraJson(evento));

        registro.headers().add("ce_specversion", VERSAO_CLOUDEVENTS.getBytes(UTF_8));
        registro.headers().add("ce_id", evento.getEventoId().getBytes(UTF_8));
        registro.headers().add("ce_source", ORIGEM.getBytes(UTF_8));
        registro.headers().add("ce_type", TIPO_DO_EVENTO.getBytes(UTF_8));
        registro.headers().add("ce_time", evento.getOcorridoEm().toString().getBytes(UTF_8));

        try {
            template.send(registro).get(PRAZO_DO_ENVIO_SEGUNDOS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("publicacao interrompida: " + evento, e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("broker nao confirmou a publicacao de " + evento, e);
        }
    }

    /** Datas em ISO-8601: o ObjectMapper do Spring Boot nao escreve Instant como numero. */
    private String paraJson(ReservaDeEstoqueCanceladaEvent evento) {
        try {
            return mapper.writeValueAsString(evento);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("nao foi possivel serializar " + evento, e);
        }
    }
}
