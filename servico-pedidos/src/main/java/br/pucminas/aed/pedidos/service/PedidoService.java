package br.pucminas.aed.pedidos.service;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import br.pucminas.aed.pedidos.domain.PedidoConfirmadoEvent;

/**
 * O servico de aplicacao do pedido.
 *
 * Hoje ele faz uma coisa so: publicar o fato. Num sistema real, e aqui que
 * moram as regras de confirmacao — validar o pedido, aplicar politica de
 * credito, registrar a confirmacao — e publicar no broker seria apenas mais um
 * passo, no fim.
 *
 * Por isso o nome e PedidoService, e nao "PedidoPublisher": ele descreve o
 * PAPEL na arquitetura, nao a unica tecnica que a classe usa hoje. Nome que
 * descreve a implementacao atual vira mentira na primeira regra nova — e a
 * propria anotacao @Service ja aponta para esse entendimento.
 *
 * SOBRE A PUBLICACAO EM SI
 * Quem publica nao conhece nenhum consumidor. Nao ha lista de destinatarios,
 * nao ha contrato de retorno, nao ha SLA de ninguem. Zero, um ou cem
 * interessados e indiferente: e exatamente isso que caracteriza o
 * desacoplamento de cardinalidade.
 */
@Service
public class PedidoService {

    private static final String TIPO_DO_EVENTO = "loja.pedido.confirmado.v1";
    private static final String ORIGEM = "/loja/servico-pedidos";
    private static final String VERSAO_CLOUDEVENTS = "1.0";

    /** Cliente que fala com o broker. O broker e o servidor Kafka, la fora. */
    private final KafkaTemplate<String, PedidoConfirmadoEvent> clienteDoBroker;
    private final String topico;

    public PedidoService(KafkaTemplate<String, PedidoConfirmadoEvent> clienteDoBroker,
                         @Value("${demo.topico}") String topico) {
        this.clienteDoBroker = clienteDoBroker;
        this.topico = topico;
    }

    /**
     * Confirma o pedido: por enquanto, so afirma o fato no barramento.
     *
     * As regras de negocio que precederiam esta publicacao entrariam aqui,
     * ANTES da chamada ao broker.
     */
    public void confirmar(PedidoConfirmadoEvent evento) {

        ProducerRecord<String, PedidoConfirmadoEvent> registro =
                new ProducerRecord<String, PedidoConfirmadoEvent>(
                        topico,
                        evento.getPedidoId(),   // mesma chave -> mesma particao -> ordem por pedido
                        evento);

        // ENVELOPE CLOUDEVENTS EM MODO BINARIO: os atributos vao nos CABECALHOS
        // da mensagem, e a carga fica no corpo. E o modo que a especificacao
        // define para mensageria — o consumidor roteia e deduplica sem precisar
        // desserializar o payload. O outro modo, o estruturado, poe tudo num
        // JSON so; e o que se costuma ver em HTTP e nos exemplos da spec.
        //
        // A especificacao 1.0 exige QUATRO atributos: specversion, id, source e
        // type. Sem os quatro, isto seria apenas "inspirado em CloudEvents".
        registro.headers().add("ce_specversion", VERSAO_CLOUDEVENTS.getBytes(UTF_8));
        registro.headers().add("ce_id", evento.getEventoId().getBytes(UTF_8));
        registro.headers().add("ce_source", ORIGEM.getBytes(UTF_8));
        registro.headers().add("ce_type", TIPO_DO_EVENTO.getBytes(UTF_8));

        // time e opcional na spec, mas aqui e essencial: e a hora em que o FATO
        // aconteceu no dominio, nao a hora em que o broker recebeu a mensagem.
        registro.headers().add("ce_time", evento.getOcorridoEm().toString().getBytes(UTF_8));

        // O retorno de send() E tratado, por um objeto com nome. Ignora-lo seria
        // deixar a falha de publicacao passar em silencio.
        PedidoCallbackService aoConcluir = new PedidoCallbackService(evento);
        clienteDoBroker.send(registro).whenComplete(aoConcluir);
    }
}
