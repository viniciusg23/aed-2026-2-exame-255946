package br.pucminas.aed.pedidos;

import java.util.Map;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import br.pucminas.aed.pedidos.domain.PedidoConfirmadoEvent;

@Configuration
public class PedidoConfig {

    private static final int PARTICOES = 3;
    private static final short REPLICAS = 1;

    /**
     * Tres particoes de proposito: na aula 02 elas servem para demonstrar
     * ordenacao por chave e paralelismo de consumo. Aqui, como todo evento usa
     * o pedidoId como chave, eventos do mesmo pedido caem sempre na mesma
     * particao — que e a unica ordem que o negocio exige.
     *
     * Uma replica so porque e um broker so. Em producao, nunca.
     */
    @Bean
    public NewTopic topicoDePedidoConfirmado(@Value("${demo.topico}") String topico) {
        return new NewTopic(topico, PARTICOES, REPLICAS);
    }

    /**
     * COMO AS DATAS SAO ESCRITAS NO TOPICO.
     *
     * Por padrao o JsonSerializer do spring-kafka usa um ObjectMapper proprio,
     * que serializa Instant como numero de epoch: 1785795247.512000000. O
     * consumidor Java le de volta sem reclamar, os testes passam, e o problema
     * so aparece quando alguem abre o topico — ou quando um consumidor escrito
     * em outra linguagem tenta interpretar aquilo.
     *
     * Duas razoes para forcar ISO-8601 aqui:
     *   1. o mesmo instante viajava em dois formatos na MESMA mensagem: ISO no
     *      cabecalho ce_time e epoch no corpo;
     *   2. o contrato do evento e o JSON no fio. Se ele depende do formato
     *      padrao de uma biblioteca Java, deixou de ser um contrato neutro.
     *
     * E um otimo exemplo de detalhe de serializacao que quebra
     * interoperabilidade e nao aparece em teste nenhum.
     */
    @Bean
    public ObjectMapper objectMapperDosEventos() {
        return JsonMapper.builder()
                .addModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .build();
    }

    /**
     * Substitui o KafkaTemplate que o Spring Boot montaria sozinho, apenas para
     * injetar o ObjectMapper acima no serializador do VALOR. O resto da
     * configuracao continua vindo do application.yml.
     */
    @Bean
    public KafkaTemplate<String, PedidoConfirmadoEvent> kafkaTemplate(
            KafkaProperties propriedades, ObjectMapper objectMapperDosEventos) {

        JsonSerializer<PedidoConfirmadoEvent> serializadorDoValor =
                new JsonSerializer<PedidoConfirmadoEvent>(objectMapperDosEventos);

        // NAO chame setAddTypeInfo(false) aqui: essa opcao ja vem do
        // application.yml (spring.json.add.type.headers). O JsonSerializer
        // recusa ser configurado pelos dois caminhos ao mesmo tempo e lanca
        // "must be configured with property setters, or via configuration
        // properties; not both" — mas so na primeira publicacao, virando 500.
        // A configuracao mora no yml; aqui so injetamos o ObjectMapper.

        Map<String, Object> config = propriedades.buildProducerProperties(null);
        // as instancias acima substituem as classes declaradas no yml
        config.remove(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
        config.remove(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);

        ProducerFactory<String, PedidoConfirmadoEvent> fabrica =
                new DefaultKafkaProducerFactory<String, PedidoConfirmadoEvent>(
                        config, new StringSerializer(), serializadorDoValor);

        return new KafkaTemplate<String, PedidoConfirmadoEvent>(fabrica);
    }
}
