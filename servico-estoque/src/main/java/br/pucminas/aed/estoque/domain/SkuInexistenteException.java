package br.pucminas.aed.estoque.domain;

/**
 * O pedido pede um SKU que este estoque nao conhece.
 *
 * No codigo de partida esta falha era engolida: o UPDATE afetava zero linhas,
 * nao lancava nada, e o evento era marcado como processado. Agora ela tem nome.
 */
public class SkuInexistenteException extends ReservaImpossivelException {

    private static final long serialVersionUID = 1L;

    private final String pedidoId;
    private final String sku;

    public SkuInexistenteException(String pedidoId, String sku) {
        super("pedido " + pedidoId + " pede o SKU " + sku + ", que nao existe no estoque");
        this.pedidoId = pedidoId;
        this.sku = sku;
    }

    public String getPedidoId() {
        return pedidoId;
    }

    public String getSku() {
        return sku;
    }
}
