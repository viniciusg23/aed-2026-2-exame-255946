package br.pucminas.aed.estoque.controller;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Devolve ao fluxo normal o que esta na DLQ — depois que a causa foi corrigida.
 *
 * DESLIGADO POR PADRAO. Reprocessar e decisao de quem operou a correcao, nao
 * algo que o servico faz sozinho. Para ligar:
 *
 *     java -jar servico-estoque.jar --estoque.dlq.reprocessar=true
 *
 * O evento volta ao topico principal com o corpo e os cabecalhos ce_*
 * ORIGINAIS; os kafka_dlt-* e dlq_* da passagem pela DLQ sao removidos. Com o
 * mesmo ce_id, a deduplicacao do EstoqueListener garante que reprocessar duas
 * vezes nao debita duas vezes — e a dos outros consumidores do topico tambem.
 *
 * A chave tambem e a original (o pedidoId): o evento volta para a particao do
 * seu pedido, e a ordem por pedido se mantem.
 *
 * O LIMITE DE REPROCESSAMENTOS existe porque, com este consumidor ligado, um
 * evento cuja causa NAO foi corrigida faria um ciclo eterno:
 * DLQ -> topico -> falha -> DLQ -> topico... O cabecalho dlq_reprocessamentos
 * viaja com o evento e corta o ciclo.
 */
@Component
public class ReprocessadorDlq {

    public static final String ID = "reprocessador-dlq";
    public static final String CABECALHO_REPROCESSAMENTOS = "dlq_reprocessamentos";

    private static final Logger log = LoggerFactory.getLogger(ReprocessadorDlq.class);
    private static final long PRAZO_DO_ENVIO_SEGUNDOS = 10;

    private final KafkaTemplate<String, String> template;
    private final String topicoOriginal;
    private final int maximoDeReprocessamentos;

    public ReprocessadorDlq(KafkaTemplate<String, String> template,
                            @Value("${demo.topico}") String topicoOriginal,
                            @Value("${estoque.dlq.maximo-de-reprocessamentos}") int maximoDeReprocessamentos) {
        this.template = template;
        this.topicoOriginal = topicoOriginal;
        this.maximoDeReprocessamentos = maximoDeReprocessamentos;
    }

    @KafkaListener(id = ID,
                   topics = "${estoque.dlq.topico}",
                   groupId = "estoque-reprocessador",
                   autoStartup = "${estoque.dlq.reprocessar:false}")
    public void reprocessar(ConsumerRecord<String, String> registro, Acknowledgment ack)
            throws Exception {

        int jaReprocessado = lerReprocessamentos(registro);
        if (jaReprocessado >= maximoDeReprocessamentos) {
            log.error("evento ja reprocessado {} vezes e falhou de novo; fica na DLQ  particao={}  offset={}",
                    jaReprocessado, registro.partition(), registro.offset());
            ack.acknowledge();
            return;
        }

        ProducerRecord<String, String> devolvido =
                new ProducerRecord<String, String>(topicoOriginal, registro.key(), registro.value());
        for (Header cabecalho : registro.headers()) {
            if (veioDoEventoOriginal(cabecalho.key())) {
                devolvido.headers().add(cabecalho);
            }
        }
        devolvido.headers().add(CABECALHO_REPROCESSAMENTOS,
                String.valueOf(jaReprocessado + 1).getBytes(UTF_8));

        // Envio sincrono: so confirma o offset da DLQ depois que o broker aceitou.
        template.send(devolvido).get(PRAZO_DO_ENVIO_SEGUNDOS, TimeUnit.SECONDS);
        ack.acknowledge();

        log.info("evento devolvido ao topico {}  chave={}  reprocessamento={}",
                topicoOriginal, registro.key(), jaReprocessado + 1);
    }

    private boolean veioDoEventoOriginal(String nome) {
        return !nome.startsWith("kafka_dlt-")
                && !nome.startsWith("dlq_");
    }

    private int lerReprocessamentos(ConsumerRecord<String, String> registro) {
        Header cabecalho = registro.headers().lastHeader(CABECALHO_REPROCESSAMENTOS);
        if (cabecalho == null) {
            return 0;
        }
        return Integer.parseInt(new String(cabecalho.value(), UTF_8));
    }
}
