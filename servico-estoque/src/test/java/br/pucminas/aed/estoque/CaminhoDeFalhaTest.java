package br.pucminas.aed.estoque;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.TestPropertySource;

import br.pucminas.aed.estoque.config.CaminhoDeFalhaConfig;
import br.pucminas.aed.estoque.controller.ReprocessadorDlq;
import br.pucminas.aed.estoque.domain.EventoMalformadoException;
import br.pucminas.aed.estoque.domain.QuantidadeInvalidaException;
import br.pucminas.aed.estoque.domain.SkuInexistenteException;
import br.pucminas.aed.estoque.service.EstoqueRepository;

/**
 * Prova automatizada da Parte A: falha permanente, falha transitoria e
 * reprocessamento a partir da DLQ. Kafka embutido e H2, sem Docker.
 *
 * O repositorio e um SPY: chama o codigo de verdade, mas conta as chamadas.
 * registrarEventoSeNovo(eventoId) roda uma vez por TENTATIVA de processar o
 * evento — e o que permite afirmar "foi tentado uma vez so" ou "foi retentado".
 *
 * As esperas de retentativa sao encurtadas aqui; a politica (limite +
 * espera exponencial) e a mesma de producao.
 */
@SpringBootTest
@EmbeddedKafka(partitions = 3, topics = { "loja.pedido.confirmado.v1", "loja.pedido.confirmado.v1.dlq" })
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.datasource.url=jdbc:h2:mem:aed-falha;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "estoque.retentativa.intervalo-inicial-ms=500",
        "estoque.retentativa.intervalo-maximo-ms=1000",
        "logging.level.br.pucminas.aed=INFO"
})
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class CaminhoDeFalhaTest {

    private static final String SKU = "TEC-002";
    private static final int ESTOQUE_INICIAL = 100;
    private static final Duration PRAZO = Duration.ofSeconds(20);
    private static final Duration JANELA_DE_OBSERVACAO = Duration.ofSeconds(3);

    @SpyBean
    private EstoqueRepository repositorio;
    @Autowired
    private JdbcTemplate jdbc;
    @Autowired
    private KafkaListenerEndpointRegistry listeners;

    @Value("${spring.embedded.kafka.brokers}")
    private String servidores;
    @Value("${demo.topico}")
    private String topico;
    @Value("${estoque.dlq.topico}")
    private String topicoDlq;

    private TestPublisher publicador;

    @BeforeEach
    void prepararEstado() {
        repositorio.redefinirEstoque(SKU, ESTOQUE_INICIAL);
        repositorio.limparEventosProcessados();
        publicador = new TestPublisher(servidores, topico);
    }

    // ------------------------------------------------------------------
    // falha permanente
    // ------------------------------------------------------------------

    @Test
    @Order(1)
    @DisplayName("A1 - SKU inexistente vai para a DLQ na primeira falha, com ce_id e motivo")
    void skuInexistenteVaiDiretoParaDlq() {
        String eventoId = UUID.randomUUID().toString();
        publicador.publicar(eventoId, "5001", "SKU-QUE-NAO-EXISTE", 1);

        ConsumerRecord<String, String> naDlq = aguardarNaDlq(eventoId);

        // os cabecalhos CloudEvents originais chegam intactos
        assertThat(cabecalho(naDlq, "ce_id")).isEqualTo(eventoId);
        assertThat(cabecalho(naDlq, "ce_type")).isEqualTo("loja.pedido.confirmado.v1");
        assertThat(cabecalho(naDlq, "ce_source")).isEqualTo("/loja/servico-pedidos");
        assertThat(cabecalho(naDlq, "ce_specversion")).isEqualTo("1.0");

        // o motivo: excecao, mensagem e instante
        assertThat(cabecalho(naDlq, CaminhoDeFalhaConfig.CABECALHO_EXCECAO))
                .isEqualTo(SkuInexistenteException.class.getName());
        assertThat(cabecalho(naDlq, CaminhoDeFalhaConfig.CABECALHO_MENSAGEM))
                .contains("SKU-QUE-NAO-EXISTE");
        assertThat(Instant.parse(cabecalho(naDlq, CaminhoDeFalhaConfig.CABECALHO_FALHOU_EM)))
                .isBeforeOrEqualTo(Instant.now());

        // o corpo e o ORIGINAL, com os campos que este servico nao declara
        assertThat(naDlq.value()).contains("\"clienteId\":\"c-9931\"");
        assertThat(naDlq.key()).isEqualTo("5001");

        // uma tentativa so: nao houve retentativa
        verify(repositorio, times(1)).registrarEventoSeNovo(eventoId);
        // e o rollback desfez o registro do eventoId: o reprocessamento nao sera
        // descartado como duplicata
        assertThat(eventoFoiRegistrado(eventoId)).isFalse();
    }

    @Test
    @Order(2)
    @DisplayName("A2 - quantidade zero vai para a DLQ sem retentar")
    void quantidadeInvalidaVaiDiretoParaDlq() {
        String eventoId = UUID.randomUUID().toString();
        publicador.publicar(eventoId, "5002", SKU, 0);

        ConsumerRecord<String, String> naDlq = aguardarNaDlq(eventoId);

        assertThat(cabecalho(naDlq, CaminhoDeFalhaConfig.CABECALHO_EXCECAO))
                .isEqualTo(QuantidadeInvalidaException.class.getName());
        verify(repositorio, times(1)).registrarEventoSeNovo(eventoId);
        confirmarQueEstoqueNaoMuda(ESTOQUE_INICIAL);
    }

    @Test
    @Order(3)
    @DisplayName("A3 - um item invalido desfaz a reserva dos itens validos do mesmo pedido")
    void itemInvalidoDesfazOPedidoInteiro() {
        String eventoId = UUID.randomUUID().toString();
        String json = "{"
                + "\"eventoId\":\"" + eventoId + "\","
                + "\"ocorridoEm\":\"2026-08-03T22:14:07.512Z\","
                + "\"pedidoId\":\"5003\","
                + "\"itens\":["
                + "{\"sku\":\"" + SKU + "\",\"quantidade\":2},"          // valido: seria debitado
                + "{\"sku\":\"SKU-QUE-NAO-EXISTE\",\"quantidade\":1}"   // invalido
                + "]}";
        publicador.publicarCorpo(eventoId, "5003", json);

        aguardarNaDlq(eventoId);

        // o TEC-002 chegou a ser debitado dentro da transacao, e o rollback devolveu
        confirmarQueEstoqueNaoMuda(ESTOQUE_INICIAL);
    }

    @Test
    @Order(4)
    @DisplayName("A4 - JSON quebrado vai para a DLQ com o corpo original")
    void jsonQuebradoVaiParaDlq() {
        String eventoId = UUID.randomUUID().toString();
        String corpoQuebrado = "{\"pedidoId\":\"5004\", isto nao e json";
        publicador.publicarCorpo(eventoId, "5004", corpoQuebrado);

        ConsumerRecord<String, String> naDlq = aguardarNaDlq(eventoId);

        assertThat(cabecalho(naDlq, CaminhoDeFalhaConfig.CABECALHO_EXCECAO))
                .isEqualTo(EventoMalformadoException.class.getName());
        assertThat(naDlq.value()).isEqualTo(corpoQuebrado);
    }

    // ------------------------------------------------------------------
    // falha transitoria
    // ------------------------------------------------------------------

    /**
     * Derrubar o H2 no meio do teste nao e pratico; renomear a tabela produz o
     * mesmo efeito para o consumidor: o banco nao atende, com uma excecao que
     * NAO e de dominio — portanto retentavel. O cenario com o Postgres de
     * verdade parado esta no roteiro do README.
     */
    @Test
    @Order(5)
    @DisplayName("A5 - banco indisponivel e retentado, e o evento e processado quando ele volta")
    void falhaTransitoriaERetentada() {
        String eventoId = UUID.randomUUID().toString();
        jdbc.execute("ALTER TABLE estoque RENAME TO estoque_fora_do_ar");
        try {
            publicador.publicar(eventoId, "5005", SKU, 1);

            // espera a primeira tentativa falhar, e o banco "volta" em seguida
            verify(repositorio, timeout(PRAZO.toMillis()).atLeast(1)).registrarEventoSeNovo(eventoId);
            esperar(Duration.ofMillis(300));
        } finally {
            jdbc.execute("ALTER TABLE estoque_fora_do_ar RENAME TO estoque");
        }

        aguardarEstoque(ESTOQUE_INICIAL - 1);
        verify(repositorio, atLeast(2)).registrarEventoSeNovo(eventoId);
        assertThat(eventoFoiRegistrado(eventoId)).isTrue();
    }

    // ------------------------------------------------------------------
    // reprocessamento
    // ------------------------------------------------------------------

    /**
     * Fica por ULTIMO: ligado, o reprocessador le a DLQ desde o inicio, e os
     * eventos invalidos dos testes anteriores tambem voltam ao topico (e a DLQ,
     * ate o limite de reprocessamentos).
     */
    @Test
    @Order(6)
    @DisplayName("A6 - corrigida a causa, o reprocessamento debita uma vez; reprocessar de novo nao debita")
    void reprocessamentoEIdempotente() {
        String eventoId = UUID.randomUUID().toString();
        String skuNovo = "SKU-" + eventoId.substring(0, 8);
        publicador.publicar(eventoId, "5006", skuNovo, 1);

        ConsumerRecord<String, String> naDlq = aguardarNaDlq(eventoId);

        // a correcao da causa: o SKU passa a existir
        jdbc.update("INSERT INTO estoque (sku, quantidade) VALUES (?, ?)", skuNovo, Integer.valueOf(10));

        MessageListenerContainer reprocessador = listeners.getListenerContainer(ReprocessadorDlq.ID);
        reprocessador.start();
        try {
            aguardarEstoque(skuNovo, 9);
        } finally {
            reprocessador.stop();
        }

        // segundo reprocessamento do MESMO evento: mesmo corpo, mesmo ce_id
        publicador.publicarCorpo(eventoId, naDlq.key(), naDlq.value());

        confirmarQueEstoqueNaoMuda(skuNovo, 9);
        assertThat(eventoFoiRegistrado(eventoId)).isTrue();
    }

    // ------------------------------------------------------------------
    private ConsumerRecord<String, String> aguardarNaDlq(String eventoId) {
        Map<String, Object> config = new HashMap<String, Object>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servidores);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "teste-dlq-" + UUID.randomUUID());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        try (KafkaConsumer<String, String> consumidor = new KafkaConsumer<String, String>(config)) {
            consumidor.subscribe(Collections.singletonList(topicoDlq));
            long limite = System.currentTimeMillis() + PRAZO.toMillis();
            while (System.currentTimeMillis() < limite) {
                for (ConsumerRecord<String, String> registro : consumidor.poll(Duration.ofMillis(500))) {
                    if (eventoId.equals(cabecalho(registro, "ce_id"))) {
                        return registro;
                    }
                }
            }
        }
        return fail("evento " + eventoId + " nao chegou a " + topicoDlq + " em " + PRAZO);
    }

    private String cabecalho(ConsumerRecord<String, String> registro, String nome) {
        Header cabecalho = registro.headers().lastHeader(nome);
        if (cabecalho == null) {
            return null;
        }
        return new String(cabecalho.value(), UTF_8);
    }

    private boolean eventoFoiRegistrado(String eventoId) {
        Long total = jdbc.queryForObject(
                "SELECT COUNT(*) FROM evento_processado WHERE evento_id = ?", Long.class, eventoId);
        return total != null && total.longValue() == 1L;
    }

    private void aguardarEstoque(int quantidadeEsperada) {
        aguardarEstoque(SKU, quantidadeEsperada);
    }

    private void aguardarEstoque(String sku, int quantidadeEsperada) {
        EstoqueVerifier verificador = new EstoqueVerifier(repositorio, sku, quantidadeEsperada);
        Awaitility.await()
                .atMost(PRAZO)
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(verificador);
    }

    private void confirmarQueEstoqueNaoMuda(int quantidadeEsperada) {
        confirmarQueEstoqueNaoMuda(SKU, quantidadeEsperada);
    }

    private void confirmarQueEstoqueNaoMuda(String sku, int quantidadeEsperada) {
        EstoqueVerifier verificador = new EstoqueVerifier(repositorio, sku, quantidadeEsperada);
        Awaitility.await()
                .during(JANELA_DE_OBSERVACAO)
                .atMost(JANELA_DE_OBSERVACAO.plusSeconds(5))
                .untilAsserted(verificador);
    }

    private void esperar(Duration tempo) {
        try {
            Thread.sleep(tempo.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
