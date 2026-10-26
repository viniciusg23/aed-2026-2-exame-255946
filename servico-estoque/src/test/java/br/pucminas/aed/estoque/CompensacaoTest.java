package br.pucminas.aed.estoque;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.TestPropertySource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import br.pucminas.aed.estoque.service.EstoqueRepository;

/**
 * Prova automatizada da Parte B: PagamentoRecusado devolve exatamente o que foi
 * reservado e publica ReservaDeEstoqueCancelada — uma vez so, e so quando
 * havia reserva. Kafka embutido e H2, sem Docker e sem o servico-pedidos.
 *
 * Cada teste usa um pedidoId proprio: os topicos sao compartilhados entre os
 * testes da classe, e o pedido e o que separa um cenario do outro.
 */
@SpringBootTest
@EmbeddedKafka(partitions = 3, topics = {
        "loja.pedido.confirmado.v1",
        "loja.pagamento.recusado.v1",
        "loja.estoque.reservacancelada.v1" })
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.datasource.url=jdbc:h2:mem:aed-compensacao;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "logging.level.br.pucminas.aed=INFO"
})
class CompensacaoTest {

    private static final String SKU = "TEC-002";
    private static final String OUTRO_SKU = "TEC-003";
    private static final int ESTOQUE_INICIAL = 100;
    private static final int ESTOQUE_INICIAL_OUTRO = 50;
    private static final Duration PRAZO = Duration.ofSeconds(20);
    private static final Duration JANELA_DE_OBSERVACAO = Duration.ofSeconds(3);

    @Autowired
    private EstoqueRepository repositorio;
    @Autowired
    private JdbcTemplate jdbc;
    @Autowired
    private ObjectMapper mapper;

    @Value("${spring.embedded.kafka.brokers}")
    private String servidores;
    @Value("${demo.topico}")
    private String topicoDePedido;
    @Value("${estoque.topico-pagamento-recusado}")
    private String topicoDeRecusa;
    @Value("${estoque.topico-reserva-cancelada}")
    private String topicoDeReservaCancelada;

    private TestPublisher publicadorDePedido;
    private KafkaTemplate<String, String> publicadorDeRecusa;

    @BeforeEach
    void prepararEstado() {
        repositorio.redefinirEstoque(SKU, ESTOQUE_INICIAL);
        jdbc.update("INSERT INTO estoque (sku, quantidade) VALUES (?, ?) ON CONFLICT DO NOTHING",
                OUTRO_SKU, Integer.valueOf(ESTOQUE_INICIAL_OUTRO));
        repositorio.redefinirEstoque(OUTRO_SKU, ESTOQUE_INICIAL_OUTRO);
        publicadorDePedido = new TestPublisher(servidores, topicoDePedido);
        publicadorDeRecusa = criarPublicadorDeRecusa();
    }

    @Test
    @DisplayName("B1 - a recusa devolve o que foi reservado e publica ReservaDeEstoqueCancelada com CloudEvents")
    void recusaDevolveEPublica() throws Exception {
        String pedidoId = novoPedido();
        publicadorDePedido.publicar(UUID.randomUUID().toString(), pedidoId, SKU, 3);
        aguardarEstoque(SKU, ESTOQUE_INICIAL - 3);

        String recusaId = UUID.randomUUID().toString();
        publicarRecusa(recusaId, pedidoId);
        aguardarEstoque(SKU, ESTOQUE_INICIAL);

        List<ConsumerRecord<String, String>> canceladas = canceladasDoPedido(pedidoId);
        assertThat(canceladas).hasSize(1);
        ConsumerRecord<String, String> cancelada = canceladas.get(0);

        // envelope CloudEvents em modo binario
        assertThat(cabecalho(cancelada, "ce_specversion")).isEqualTo("1.0");
        assertThat(cabecalho(cancelada, "ce_type")).isEqualTo("loja.estoque.reservacancelada.v1");
        assertThat(cabecalho(cancelada, "ce_source")).isEqualTo("/loja/servico-estoque");
        assertThat(cabecalho(cancelada, "ce_id")).isNotBlank();
        Instant.parse(cabecalho(cancelada, "ce_time"));

        // corpo: o pedido, a causa e o que foi devolvido; data em ISO-8601
        JsonNode corpo = mapper.readTree(cancelada.value());
        assertThat(corpo.get("eventoId").asText()).isEqualTo(cabecalho(cancelada, "ce_id"));
        assertThat(corpo.get("pedidoId").asText()).isEqualTo(pedidoId);
        assertThat(corpo.get("pagamentoRecusadoId").asText()).isEqualTo(recusaId);
        assertThat(corpo.get("ocorridoEm").isTextual()).isTrue();
        Instant.parse(corpo.get("ocorridoEm").asText());
        assertThat(corpo.get("itens")).hasSize(1);
        assertThat(corpo.get("itens").get(0).get("sku").asText()).isEqualTo(SKU);
        assertThat(corpo.get("itens").get(0).get("quantidade").asInt()).isEqualTo(3);

        // nada foi apagado: a reserva continua registrada, ao lado da recusa
        assertThat(contar("SELECT COUNT(*) FROM reserva WHERE pedido_id = ?", pedidoId)).isEqualTo(1L);
        assertThat(contar("SELECT COUNT(*) FROM pagamento_recusado WHERE pedido_id = ? AND reserva_devolvida",
                pedidoId)).isEqualTo(1L);
    }

    @Test
    @DisplayName("B2 - devolve exatamente o que reservou, item por item")
    void devolveCadaItem() {
        String pedidoId = novoPedido();
        String eventoId = UUID.randomUUID().toString();
        String json = "{"
                + "\"eventoId\":\"" + eventoId + "\","
                + "\"ocorridoEm\":\"2026-08-03T22:14:07.512Z\","
                + "\"pedidoId\":\"" + pedidoId + "\","
                + "\"itens\":["
                + "{\"sku\":\"" + SKU + "\",\"quantidade\":2},"
                + "{\"sku\":\"" + OUTRO_SKU + "\",\"quantidade\":5},"
                + "{\"sku\":\"" + SKU + "\",\"quantidade\":1}"
                + "]}";
        publicadorDePedido.publicarCorpo(eventoId, pedidoId, json);
        aguardarEstoque(SKU, ESTOQUE_INICIAL - 3);
        aguardarEstoque(OUTRO_SKU, ESTOQUE_INICIAL_OUTRO - 5);

        publicarRecusa(UUID.randomUUID().toString(), pedidoId);

        aguardarEstoque(SKU, ESTOQUE_INICIAL);
        aguardarEstoque(OUTRO_SKU, ESTOQUE_INICIAL_OUTRO);
    }

    @Test
    @DisplayName("B3 - a MESMA recusa entregue duas vezes devolve uma vez so")
    void mesmaRecusaDuasVezes() {
        String pedidoId = novoPedido();
        publicadorDePedido.publicar(UUID.randomUUID().toString(), pedidoId, SKU, 2);
        aguardarEstoque(SKU, ESTOQUE_INICIAL - 2);

        String recusaId = UUID.randomUUID().toString();
        publicarRecusa(recusaId, pedidoId);
        aguardarEstoque(SKU, ESTOQUE_INICIAL);

        publicarRecusa(recusaId, pedidoId);   // reentrega: mesmo ce_id

        confirmarQueEstoqueNaoMuda(SKU, ESTOQUE_INICIAL);
        assertThat(canceladasDoPedido(pedidoId)).hasSize(1);
    }

    @Test
    @DisplayName("B4 - outra recusa do mesmo pedido, com outro ce_id, tambem nao devolve de novo")
    void outraRecusaDoMesmoPedido() {
        String pedidoId = novoPedido();
        publicadorDePedido.publicar(UUID.randomUUID().toString(), pedidoId, SKU, 2);
        aguardarEstoque(SKU, ESTOQUE_INICIAL - 2);

        publicarRecusa(UUID.randomUUID().toString(), pedidoId);
        aguardarEstoque(SKU, ESTOQUE_INICIAL);

        publicarRecusa(UUID.randomUUID().toString(), pedidoId);   // outro ce_id, mesmo pedido

        confirmarQueEstoqueNaoMuda(SKU, ESTOQUE_INICIAL);
        assertThat(canceladasDoPedido(pedidoId)).hasSize(1);
    }

    @Test
    @DisplayName("B5 - recusa de pedido sem reserva e registrada, nao devolve nada e nao publica")
    void recusaSemReserva() {
        String pedidoId = novoPedido();

        publicarRecusa(UUID.randomUUID().toString(), pedidoId);

        Awaitility.await()
                .atMost(PRAZO)
                .pollInterval(Duration.ofMillis(200))
                .until(new RecusaRegistrada(jdbc, pedidoId));
        assertThat(contar("SELECT COUNT(*) FROM pagamento_recusado WHERE pedido_id = ? AND NOT reserva_devolvida",
                pedidoId)).isEqualTo(1L);
        confirmarQueEstoqueNaoMuda(SKU, ESTOQUE_INICIAL);
        assertThat(canceladasDoPedido(pedidoId)).isEmpty();
    }

    @Test
    @DisplayName("B6 - PedidoConfirmado que chega DEPOIS da recusa nao reserva")
    void pedidoConfirmadoDepoisDaRecusa() {
        String pedidoId = novoPedido();
        publicarRecusa(UUID.randomUUID().toString(), pedidoId);
        Awaitility.await()
                .atMost(PRAZO)
                .pollInterval(Duration.ofMillis(200))
                .until(new RecusaRegistrada(jdbc, pedidoId));

        String eventoId = UUID.randomUUID().toString();
        publicadorDePedido.publicar(eventoId, pedidoId, SKU, 4);

        Awaitility.await()
                .atMost(PRAZO)
                .pollInterval(Duration.ofMillis(200))
                .until(new EventoProcessado(jdbc, eventoId));
        confirmarQueEstoqueNaoMuda(SKU, ESTOQUE_INICIAL);
        assertThat(contar("SELECT COUNT(*) FROM reserva WHERE pedido_id = ?", pedidoId)).isZero();
    }

    // ------------------------------------------------------------------
    private String novoPedido() {
        return "P-" + UUID.randomUUID().toString().substring(0, 8);
    }

    /** Publica PagamentoRecusado como o servico-pedidos publica: JSON e cabecalhos ce_*. */
    private void publicarRecusa(String eventoId, String pedidoId) {
        String json = "{"
                + "\"eventoId\":\"" + eventoId + "\","
                + "\"ocorridoEm\":\"2026-08-03T22:20:00.000Z\","
                + "\"pedidoId\":\"" + pedidoId + "\","
                + "\"motivo\":\"cartao recusado pela operadora\""
                + "}";
        ProducerRecord<String, String> registro =
                new ProducerRecord<String, String>(topicoDeRecusa, pedidoId, json);
        registro.headers().add("ce_specversion", "1.0".getBytes(UTF_8));
        registro.headers().add("ce_id", eventoId.getBytes(UTF_8));
        registro.headers().add("ce_source", "/loja/servico-pagamentos".getBytes(UTF_8));
        registro.headers().add("ce_type", "loja.pagamento.recusado.v1".getBytes(UTF_8));
        registro.headers().add("ce_time", "2026-08-03T22:20:00.000Z".getBytes(UTF_8));
        publicadorDeRecusa.send(registro);
        publicadorDeRecusa.flush();
    }

    private KafkaTemplate<String, String> criarPublicadorDeRecusa() {
        Map<String, Object> config = new HashMap<String, Object>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servidores);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new KafkaTemplate<String, String>(new DefaultKafkaProducerFactory<String, String>(config));
    }

    /**
     * Le o topico de ReservaDeEstoqueCancelada desde o inicio, durante a janela
     * de observacao, e devolve os eventos do pedido.
     */
    private List<ConsumerRecord<String, String>> canceladasDoPedido(String pedidoId) {
        Map<String, Object> config = new HashMap<String, Object>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servidores);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "teste-canceladas-" + UUID.randomUUID());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        List<ConsumerRecord<String, String>> doPedido = new ArrayList<ConsumerRecord<String, String>>();
        try (KafkaConsumer<String, String> consumidor = new KafkaConsumer<String, String>(config)) {
            consumidor.subscribe(Collections.singletonList(topicoDeReservaCancelada));
            long limite = System.currentTimeMillis() + JANELA_DE_OBSERVACAO.toMillis();
            while (System.currentTimeMillis() < limite) {
                for (ConsumerRecord<String, String> registro : consumidor.poll(Duration.ofMillis(500))) {
                    if (pedidoId.equals(registro.key())) {
                        doPedido.add(registro);
                    }
                }
            }
        }
        return doPedido;
    }

    private String cabecalho(ConsumerRecord<String, String> registro, String nome) {
        Header cabecalho = registro.headers().lastHeader(nome);
        if (cabecalho == null) {
            return null;
        }
        return new String(cabecalho.value(), UTF_8);
    }

    private long contar(String sql, String pedidoId) {
        Long total = jdbc.queryForObject(sql, Long.class, pedidoId);
        return total == null ? 0L : total.longValue();
    }

    private void aguardarEstoque(String sku, int quantidadeEsperada) {
        EstoqueVerifier verificador = new EstoqueVerifier(repositorio, sku, quantidadeEsperada);
        Awaitility.await()
                .atMost(PRAZO)
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(verificador);
    }

    private void confirmarQueEstoqueNaoMuda(String sku, int quantidadeEsperada) {
        EstoqueVerifier verificador = new EstoqueVerifier(repositorio, sku, quantidadeEsperada);
        Awaitility.await()
                .during(JANELA_DE_OBSERVACAO)
                .atMost(JANELA_DE_OBSERVACAO.plusSeconds(5))
                .untilAsserted(verificador);
    }

    /** Condicao do Awaitility: a recusa do pedido ja foi registrada. */
    private static class RecusaRegistrada implements Callable<Boolean> {
        private final JdbcTemplate jdbc;
        private final String pedidoId;

        RecusaRegistrada(JdbcTemplate jdbc, String pedidoId) {
            this.jdbc = jdbc;
            this.pedidoId = pedidoId;
        }

        @Override
        public Boolean call() {
            Long total = jdbc.queryForObject(
                    "SELECT COUNT(*) FROM pagamento_recusado WHERE pedido_id = ?", Long.class, pedidoId);
            return Boolean.valueOf(total != null && total.longValue() == 1L);
        }
    }

    /** Condicao do Awaitility: o evento ja foi processado (e registrado na deduplicacao). */
    private static class EventoProcessado implements Callable<Boolean> {
        private final JdbcTemplate jdbc;
        private final String eventoId;

        EventoProcessado(JdbcTemplate jdbc, String eventoId) {
            this.jdbc = jdbc;
            this.eventoId = eventoId;
        }

        @Override
        public Boolean call() {
            Long total = jdbc.queryForObject(
                    "SELECT COUNT(*) FROM evento_processado WHERE evento_id = ?", Long.class, eventoId);
            return Boolean.valueOf(total != null && total.longValue() == 1L);
        }
    }
}
