package br.pucminas.aed.pedidos.service;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import br.pucminas.aed.pedidos.domain.PagamentoRecusadoEvent;

/**
 * Simula o servico de pagamento: afirma que o pagamento de um pedido foi
 * recusado.
 *
 * Mesmo envelope do PedidoConfirmado: CloudEvents em modo binario, com os
 * quatro atributos obrigatorios e o time. A chave e o pedidoId, como no
 * PedidoConfirmado — eventos do mesmo pedido mantem a ordem dentro de cada
 * topico.
 *
 * Repare que nao ha nenhuma chamada ao servico-estoque. A devolucao da reserva
 * nao e pedida a ninguem: o estoque reage ao fato.
 */
@Service
public class PagamentoService {

    private static final String TIPO_DO_EVENTO = "loja.pagamento.recusado.v1";
    private static final String ORIGEM = "/loja/servico-pagamentos";
    private static final String VERSAO_CLOUDEVENTS = "1.0";

    private final KafkaTemplate<String, PagamentoRecusadoEvent> clienteDoBroker;
    private final String topico;

    public PagamentoService(KafkaTemplate<String, PagamentoRecusadoEvent> clienteDoBroker,
                            @Value("${demo.topico-pagamento-recusado}") String topico) {
        this.clienteDoBroker = clienteDoBroker;
        this.topico = topico;
    }

    public void recusar(PagamentoRecusadoEvent evento) {

        ProducerRecord<String, PagamentoRecusadoEvent> registro =
                new ProducerRecord<String, PagamentoRecusadoEvent>(
                        topico,
                        evento.getPedidoId(),
                        evento);

        registro.headers().add("ce_specversion", VERSAO_CLOUDEVENTS.getBytes(UTF_8));
        registro.headers().add("ce_id", evento.getEventoId().getBytes(UTF_8));
        registro.headers().add("ce_source", ORIGEM.getBytes(UTF_8));
        registro.headers().add("ce_type", TIPO_DO_EVENTO.getBytes(UTF_8));
        registro.headers().add("ce_time", evento.getOcorridoEm().toString().getBytes(UTF_8));

        PagamentoCallbackService aoConcluir = new PagamentoCallbackService(evento);
        clienteDoBroker.send(registro).whenComplete(aoConcluir);
    }
}
