package br.pucminas.aed.estoque;

import static org.assertj.core.api.Assertions.assertThat;

import org.awaitility.core.ThrowingRunnable;

import br.pucminas.aed.estoque.service.EstoqueRepository;

/**
 * Verificacao usada pelo Awaitility, escrita como classe em vez de lambda.
 *
 * O Awaitility repete esta verificacao ate ela passar (ou ate estourar o
 * prazo). Como o consumo do evento e ASSINCRONO, nao da para conferir o
 * estoque logo depois de publicar: a reserva ainda nao aconteceu. Esperar por
 * uma condicao, e nao por um tempo fixo, e o que torna o teste confiavel.
 */
public class EstoqueVerifier implements ThrowingRunnable {

    private final EstoqueRepository repositorio;
    private final String sku;
    private final int quantidadeEsperada;

    public EstoqueVerifier(EstoqueRepository repositorio, String sku, int quantidadeEsperada) {
        this.repositorio = repositorio;
        this.sku = sku;
        this.quantidadeEsperada = quantidadeEsperada;
    }

    @Override
    public void run() {
        int atual = repositorio.quantidadeEmEstoque(sku);
        assertThat(atual)
                .as("quantidade em estoque do SKU %s", sku)
                .isEqualTo(quantidadeEsperada);
    }
}
