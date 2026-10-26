package br.pucminas.aed.estoque.service;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import br.pucminas.aed.estoque.domain.ItemDoPedidoVO;
import br.pucminas.aed.estoque.domain.PedidoConfirmadoEvent;
import br.pucminas.aed.estoque.domain.QuantidadeInvalidaException;
import br.pucminas.aed.estoque.domain.SkuInexistenteException;

/**
 * O coracao da demonstracao.
 *
 * DETALHE QUE DERRUBA A IDEMPOTENCIA NA PRATICA: o registro da chave de
 * deduplicacao e o efeito de negocio precisam estar na MESMA transacao. Em
 * transacoes separadas existe uma janela em que o processo morre entre as duas
 * e o evento e reprocessado — exatamente o que se queria evitar.
 *
 * Por isso o @Transactional esta AQUI, e nao no metodo do listener: assim a
 * confirmacao do offset (ack) so acontece depois que este commit terminou.
 */
@Service
public class EstoqueService {

    private static final Logger log = LoggerFactory.getLogger(EstoqueService.class);

    private final EstoqueRepository repositorio;
    private final ModoDaDemo modo;

    public EstoqueService(EstoqueRepository repositorio, ModoDaDemo modo) {
        this.repositorio = repositorio;
        this.modo = modo;
    }

    /**
     * @return true se o efeito de negocio foi aplicado; false se era duplicata.
     */
    @Transactional
    public boolean processar(String eventoId, PedidoConfirmadoEvent evento) {

        if (modo.isDeduplicacaoAtiva()) {
            boolean primeiraVez = repositorio.registrarEventoSeNovo(eventoId);
            if (!primeiraVez) {
                log.info("evento {} JA PROCESSADO, descartando em silencio", eventoId);
                return false;
            }
        } else {
            log.warn("deduplicacao DESLIGADA - processando {} sem verificar", eventoId);
        }

        reservarItens(eventoId, evento);

        log.info("reserva aplicada  evento={}  pedido={}", eventoId, evento.getPedidoId());
        return true;
    }

    /**
     * Percorrer os itens e orquestracao, e por isso mora aqui, nao no repositorio.
     *
     * As falhas permanentes sao lancadas DENTRO da transacao de processar(), e
     * isso e o que importa: o rollback desfaz o registro do eventoId e qualquer
     * item ja debitado deste mesmo pedido. Se o eventoId ficasse gravado, o
     * evento reprocessado a partir da DLQ seria descartado como duplicata.
     *
     * Cada item debitado e registrado na tabela reserva, na mesma transacao:
     * o debito e o registro do que devolver acontecem juntos ou nao acontecem.
     */
    private void reservarItens(String eventoId, PedidoConfirmadoEvent evento) {
        List<ItemDoPedidoVO> itens = evento.getItens();
        for (ItemDoPedidoVO item : itens) {
            if (item.getQuantidade() <= 0) {
                throw new QuantidadeInvalidaException(
                        evento.getPedidoId(), item.getSku(), item.getQuantidade());
            }
            int linhas = repositorio.reservar(item.getSku(), item.getQuantidade());
            if (linhas == 0) {
                throw new SkuInexistenteException(evento.getPedidoId(), item.getSku());
            }
            repositorio.registrarReserva(evento.getPedidoId(), item.getSku(), item.getQuantidade(), eventoId);
        }
    }
}
