package br.pucminas.aed.estoque.domain;

/**
 * A mensagem chegou, mas nao da para le-la como PedidoConfirmado: JSON
 * quebrado, campo obrigatorio ausente, ou sem o cabecalho ce_id.
 *
 * E a "poison pill" classica. Retentar le os mesmos bytes e falha igual — e,
 * sem limite, trava a particao inteira atras dela.
 */
public class EventoMalformadoException extends ReservaImpossivelException {

    private static final long serialVersionUID = 1L;

    public EventoMalformadoException(String mensagem) {
        super(mensagem);
    }

    public EventoMalformadoException(String mensagem, Throwable causa) {
        super(mensagem, causa);
    }
}
