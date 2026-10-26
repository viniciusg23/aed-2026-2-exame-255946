package br.pucminas.aed.estoque.domain;

/**
 * Raiz das falhas PERMANENTES da reserva de estoque.
 *
 * Permanente quer dizer: tentar de novo da o mesmo erro. Um SKU que nao existe
 * continua nao existindo no segundo seguinte; uma quantidade negativa continua
 * negativa. Retentar so atrasa a fila e esconde o problema.
 *
 * POR QUE UMA HIERARQUIA
 * O tratador de erro do consumidor declara UMA classe como "nao retentavel" —
 * esta. Cada motivo concreto e uma subclasse, e um motivo novo entra no caminho
 * da DLQ so por herdar daqui, sem tocar na configuracao do Kafka.
 *
 * O que NAO herda daqui e, por omissao, transitorio: banco fora do ar, timeout
 * de conexao, broker indisponivel. Esses sao retentados.
 */
public abstract class ReservaImpossivelException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    protected ReservaImpossivelException(String mensagem) {
        super(mensagem);
    }

    protected ReservaImpossivelException(String mensagem, Throwable causa) {
        super(mensagem, causa);
    }
}
