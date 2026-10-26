package br.pucminas.aed.estoque.domain;

/**
 * O pedido pede zero, ou menos que zero, unidades de um SKU.
 *
 * Sem esta verificacao, uma quantidade negativa AUMENTARIA o estoque — o
 * UPDATE subtrai um numero negativo. Nao ha erro de banco que pegue isso.
 */
public class QuantidadeInvalidaException extends ReservaImpossivelException {

    private static final long serialVersionUID = 1L;

    private final String pedidoId;
    private final String sku;
    private final int quantidade;

    public QuantidadeInvalidaException(String pedidoId, String sku, int quantidade) {
        super("pedido " + pedidoId + " pede quantidade " + quantidade
                + " do SKU " + sku + "; a quantidade precisa ser maior que zero");
        this.pedidoId = pedidoId;
        this.sku = sku;
        this.quantidade = quantidade;
    }

    public String getPedidoId() {
        return pedidoId;
    }

    public String getSku() {
        return sku;
    }

    public int getQuantidade() {
        return quantidade;
    }
}
