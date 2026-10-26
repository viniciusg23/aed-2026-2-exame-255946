package br.pucminas.aed.estoque.service;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import br.pucminas.aed.estoque.domain.ItemDoPedidoVO;
import br.pucminas.aed.estoque.domain.PedidoConfirmadoEvent;

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

        reservarItens(evento);

        log.info("reserva aplicada  evento={}  pedido={}", eventoId, evento.getPedidoId());
        return true;
    }

    /** Percorrer os itens e orquestracao, e por isso mora aqui, nao no repositorio. */
    private void reservarItens(PedidoConfirmadoEvent evento) {
        List<ItemDoPedidoVO> itens = evento.getItens();
        for (ItemDoPedidoVO item : itens) {
            repositorio.reservar(item.getSku(), item.getQuantidade());
        }
    }
}
