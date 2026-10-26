package br.pucminas.aed.estoque;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.UUID;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.TestPropertySource;

import br.pucminas.aed.estoque.service.EstoqueRepository;
import br.pucminas.aed.estoque.service.ModoDaDemo;

/**
 * Prova automatizada dos tres passos da demonstracao - roda com Kafka embutido
 * e H2, portanto SEM Docker e SEM o servico-pedidos.
 *
 * Testar o consumidor sem subir o produtor e justamente o que a separacao em
 * duas aplicacoes permite: os dois lados so se conhecem pelo topico.
 */
@SpringBootTest
@EmbeddedKafka(partitions = 3, topics = "loja.pedido.confirmado.v1")
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.datasource.url=jdbc:h2:mem:aed;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "logging.level.br.pucminas.aed=INFO"
})
class IdempotenciaTest {

    private static final String SKU = "TEC-002";
    private static final int ESTOQUE_INICIAL = 100;
    private static final Duration PRAZO = Duration.ofSeconds(20);
    private static final Duration JANELA_DE_OBSERVACAO = Duration.ofSeconds(3);

    @Autowired
    private EstoqueRepository repositorio;
    @Autowired
    private ModoDaDemo modo;

    @Value("${spring.embedded.kafka.brokers}")
    private String servidores;
    @Value("${demo.topico}")
    private String topico;

    private TestPublisher publicador;

    @BeforeEach
    void prepararEstado() {
        repositorio.redefinirEstoque(SKU, ESTOQUE_INICIAL);
        repositorio.limparEventosProcessados();
        modo.setDeduplicacaoAtiva(true);
        publicador = new TestPublisher(servidores, topico);
    }

    @Test
    @DisplayName("1 - evento novo reserva estoque uma vez")
    void eventoNovoReservaUmaVez() {
        publicador.publicar(UUID.randomUUID().toString(), "4711", SKU, 1);

        aguardarEstoque(99);
        assertThat(repositorio.contarEventosProcessados()).isEqualTo(1L);
    }

    @Test
    @DisplayName("2 - o MESMO evento entregue tres vezes reserva uma so vez")
    void reentregaNaoDuplicaOEfeito() {
        String eventoId = UUID.randomUUID().toString();

        publicador.publicar(eventoId, "4712", SKU, 1);
        aguardarEstoque(99);

        publicador.publicar(eventoId, "4712", SKU, 1);   // reentrega: mesmo eventoId
        publicador.publicar(eventoId, "4712", SKU, 1);   // e mais uma

        confirmarQueEstoqueNaoMuda(99);
        assertThat(repositorio.contarEventosProcessados()).isEqualTo(1L);
    }

    @Test
    @DisplayName("3 - sem deduplicacao, a mesma entrega debita o estoque de novo")
    void semDeduplicacaoOEfeitoDuplica() {
        String eventoId = UUID.randomUUID().toString();

        publicador.publicar(eventoId, "4713", SKU, 1);
        aguardarEstoque(99);

        modo.setDeduplicacaoAtiva(false);   // o que o passo 3 do roteiro demonstra
        publicador.publicar(eventoId, "4713", SKU, 1);

        aguardarEstoque(98);                // cobranca dupla: o incidente
    }

    @Test
    @DisplayName("4 - campos que o consumidor nao declara sao ignorados")
    void consumidorTolerante() {
        // O JSON publicado tem clienteId, valorTotal, moeda e preco — nenhum
        // deles existe nas classes deste servico. Se o consumidor nao fosse
        // tolerante, a desserializacao falharia e o estoque nao mudaria.
        publicador.publicar(UUID.randomUUID().toString(), "4714", SKU, 3);

        aguardarEstoque(97);
    }

    // ------------------------------------------------------------------
    private void aguardarEstoque(int quantidadeEsperada) {
        EstoqueVerifier verificador = new EstoqueVerifier(repositorio, SKU, quantidadeEsperada);
        Awaitility.await()
                .atMost(PRAZO)
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(verificador);
    }

    private void confirmarQueEstoqueNaoMuda(int quantidadeEsperada) {
        EstoqueVerifier verificador = new EstoqueVerifier(repositorio, SKU, quantidadeEsperada);
        Awaitility.await()
                .during(JANELA_DE_OBSERVACAO)
                .atMost(JANELA_DE_OBSERVACAO.plusSeconds(5))
                .untilAsserted(verificador);
    }
}
