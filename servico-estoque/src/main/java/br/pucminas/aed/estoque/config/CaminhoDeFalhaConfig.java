package br.pucminas.aed.estoque.config;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.time.Instant;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

import br.pucminas.aed.estoque.domain.ReservaImpossivelException;

/**
 * O caminho de falha do consumidor, escrito por extenso.
 *
 * No codigo de partida esta decisao era tomada por omissao: o Spring Kafka
 * retentava 10 vezes sem espera e depois so registrava o erro no log e seguia
 * em frente — o evento sumia. Aqui ela e explicita:
 *
 *   TRANSITORIA  qualquer excecao que NAO herde de ReservaImpossivelException
 *                (Postgres fora do ar, timeout de conexao). Retenta com espera
 *                exponencial e com LIMITE; esgotado o limite, vai para a DLQ.
 *
 *   PERMANENTE   ReservaImpossivelException e subclasses. Nao retenta: vai
 *                direto para a DLQ na primeira falha.
 *
 * O Spring Boot aplica sozinho o unico CommonErrorHandler declarado como bean
 * a fabrica de containers dos @KafkaListener. Nao ha mais nada a ligar.
 */
@Configuration
public class CaminhoDeFalhaConfig {

    // O motivo da falha, gravado junto com o evento na DLQ. Prefixo proprio para
    // nao colidir com os kafka_dlt-* do Spring.
    public static final String CABECALHO_EXCECAO = "dlq_excecao";
    public static final String CABECALHO_MENSAGEM = "dlq_mensagem";
    /** Instante da falha, em ISO-8601. */
    public static final String CABECALHO_FALHOU_EM = "dlq_falhou_em";

    /** Cada topico consumido tem a sua DLQ: o mesmo nome, com este sufixo. */
    private static final String SUFIXO_DLQ = ".dlq";

    private static final Logger log = LoggerFactory.getLogger(CaminhoDeFalhaConfig.class);

    /**
     * As DLQs sao declaradas aqui, com o MESMO numero de particoes dos topicos
     * de origem. O recoverer publica na mesma particao de onde o evento veio;
     * se o broker criasse a DLQ sozinho, ela nasceria com uma particao so e o
     * envio para as particoes 1 e 2 falharia.
     */
    @Bean
    public NewTopic topicoDaDlq(@Value("${estoque.dlq.topico}") String topico,
                                @Value("${estoque.dlq.particoes}") int particoes) {
        return new NewTopic(topico, particoes, (short) 1);
    }

    @Bean
    public NewTopic topicoDaDlqDePagamentoRecusado(
            @Value("${estoque.dlq.topico-pagamento-recusado}") String topico,
            @Value("${estoque.dlq.particoes}") int particoes) {
        return new NewTopic(topico, particoes, (short) 1);
    }

    /** O topico da compensacao, criado por quem publica. */
    @Bean
    public NewTopic topicoDeReservaCancelada(
            @Value("${estoque.topico-reserva-cancelada}") String topico,
            @Value("${estoque.dlq.particoes}") int particoes) {
        return new NewTopic(topico, particoes, (short) 1);
    }

    @Bean
    public DefaultErrorHandler tratadorDeErro(
            KafkaTemplate<String, String> template,
            @Value("${estoque.retentativa.maximo}") int maximoDeRetentativas,
            @Value("${estoque.retentativa.intervalo-inicial-ms}") long intervaloInicial,
            @Value("${estoque.retentativa.multiplicador}") double multiplicador,
            @Value("${estoque.retentativa.intervalo-maximo-ms}") long intervaloMaximo) {

        // O recoverer copia todos os cabecalhos do evento original — os ce_*,
        // ce_id inclusive — e acrescenta os kafka_dlt-* com o motivo:
        // exception-fqcn, exception-message, exception-stacktrace,
        // original-topic, original-partition, original-offset.
        DeadLetterPublishingRecoverer recoverer =
                new DeadLetterPublishingRecoverer(template, this::destinoNaDlq);
        recoverer.setHeadersFunction(this::cabecalhosDoMotivo);

        ExponentialBackOffWithMaxRetries espera = new ExponentialBackOffWithMaxRetries(maximoDeRetentativas);
        espera.setInitialInterval(intervaloInicial);
        espera.setMultiplier(multiplicador);
        espera.setMaxInterval(intervaloMaximo);

        DefaultErrorHandler tratador = new DefaultErrorHandler(recoverer, espera);
        tratador.addNotRetryableExceptions(ReservaImpossivelException.class);
        // Confirma o offset do evento depois que ele foi para a DLQ. O padrao e
        // NAO confirmar: o evento continuaria "nao lido" para o grupo, e a cada
        // reinicio seria consumido e mandado para a DLQ de novo. So funciona
        // com ack-mode manual_immediate, que e o deste servico.
        tratador.setCommitRecovered(true);
        tratador.setRetryListeners(this::registrarTentativa);
        return tratador;
    }

    /**
     * loja.pedido.confirmado.v1  -> loja.pedido.confirmado.v1.dlq
     * loja.pagamento.recusado.v1 -> loja.pagamento.recusado.v1.dlq
     *
     * Um evento que falha ao ser lido DA PROPRIA DLQ (pelo ReprocessadorDlq)
     * volta para a mesma DLQ, e nao para uma ".dlq.dlq".
     */
    private TopicPartition destinoNaDlq(ConsumerRecord<?, ?> registro, Exception erro) {
        String origem = registro.topic();
        String destino = origem.endsWith(SUFIXO_DLQ) ? origem : origem + SUFIXO_DLQ;
        log.error("evento enviado para a DLQ  topico={}  particao={}  offset={}  motivo={}",
                origem, registro.partition(), registro.offset(), motivo(erro).getMessage());
        return new TopicPartition(destino, registro.partition());
    }

    /**
     * O motivo, em cabecalhos proprios, por dois motivos:
     *
     *   - kafka_dlt-original-timestamp e o instante em que o evento foi
     *     PRODUZIDO, nao o da falha;
     *   - kafka_dlt-exception-fqcn e kafka_dlt-exception-message descrevem a
     *     ListenerExecutionFailedException com que o Spring embrulha o erro do
     *     listener, e nao a excecao de dominio que de fato explica a falha.
     */
    private Headers cabecalhosDoMotivo(ConsumerRecord<?, ?> registro, Exception erro) {
        Throwable motivo = motivo(erro);
        String mensagem = motivo.getMessage() == null ? "" : motivo.getMessage();

        RecordHeaders cabecalhos = new RecordHeaders();
        cabecalhos.add(CABECALHO_EXCECAO, motivo.getClass().getName().getBytes(UTF_8));
        cabecalhos.add(CABECALHO_MENSAGEM, mensagem.getBytes(UTF_8));
        cabecalhos.add(CABECALHO_FALHOU_EM, Instant.now().toString().getBytes(UTF_8));
        return cabecalhos;
    }

    /**
     * A excecao de dominio, se houver uma na cadeia; senao, a causa mais
     * profunda (a do driver JDBC, por exemplo).
     */
    private Throwable motivo(Exception erro) {
        Throwable atual = erro;
        while (atual != null) {
            if (atual instanceof ReservaImpossivelException) {
                return atual;
            }
            atual = atual.getCause();
        }
        return NestedExceptionUtils.getMostSpecificCause(erro);
    }

    private void registrarTentativa(ConsumerRecord<?, ?> registro, Exception erro, int tentativa) {
        log.warn("falha na tentativa {}  topico={}  particao={}  offset={}  motivo={}",
                tentativa, registro.topic(), registro.partition(), registro.offset(), motivo(erro).getMessage());
    }
}
