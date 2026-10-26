package br.pucminas.aed.estoque.service;

import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Interruptor da demonstracao.
 *
 * Existe para o passo 3 do roteiro: mostrar o estoque sendo debitado duas vezes
 * quando a deduplicacao nao esta la. Fora de uma sala de aula isto nao
 * existiria — consumidor idempotente nao e opcional.
 *
 * Como o servico nao expoe HTTP, o valor vem da propriedade na subida:
 *
 *     java -jar servico-estoque.jar --demo.deduplicacao-ativa=false
 *
 * O AtomicBoolean nao e capricho: o valor pode ser alterado pelos testes numa
 * thread e lido pela thread do consumidor Kafka em outra. Um boolean comum nao
 * garante que a segunda enxergue o que a primeira escreveu.
 */
@Component
public class ModoDaDemo {

    private final AtomicBoolean deduplicacaoAtiva;

    public ModoDaDemo(@Value("${demo.deduplicacao-ativa:true}") boolean valorInicial) {
        this.deduplicacaoAtiva = new AtomicBoolean(valorInicial);
    }

    public boolean isDeduplicacaoAtiva() {
        return deduplicacaoAtiva.get();
    }

    public void setDeduplicacaoAtiva(boolean ativa) {
        this.deduplicacaoAtiva.set(ativa);
    }
}
