package br.pucminas.aed.estoque;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

/**
 * Coloca eventos no topico, para exercitar o consumidor.
 *
 * Repare que o teste NAO importa nada do servico-pedidos. Ele publica JSON cru,
 * exatamente como qualquer produtor faria — inclusive um escrito em outra
 * linguagem. E assim que se testa um consumidor de eventos: pelo contrato do
 * fio, nunca por uma classe compartilhada.
 *
 * Publicamos String de proposito, e nao objeto: assim o teste enxerga o mesmo
 * que o broker enxerga.
 */
public class TestPublisher {

    private final KafkaTemplate<String, String> template;
    private final String topico;

    public TestPublisher(String servidores, String topico) {
        Map<String, Object> config = new HashMap<String, Object>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servidores);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        ProducerFactory<String, String> fabrica = new DefaultKafkaProducerFactory<String, String>(config);
        this.template = new KafkaTemplate<String, String>(fabrica);
        this.topico = topico;
    }

    /**
     * Publica o evento como JSON, com o eventoId no cabecalho ce_id — do jeito
     * que o servico-pedidos publica.
     */
    public void publicar(String eventoId, String pedidoId, String sku, int quantidade) {
        String json = "{"
                + "\"eventoId\":\"" + eventoId + "\","
                + "\"ocorridoEm\":\"2026-08-03T22:14:07.512Z\","
                + "\"pedidoId\":\"" + pedidoId + "\","
                + "\"clienteId\":\"c-9931\","          // campo que o consumidor NAO declara
                + "\"valorTotal\":289.90,"             // idem
                + "\"moeda\":\"BRL\","                 // idem
                + "\"itens\":[{\"sku\":\"" + sku + "\","
                + "\"quantidade\":" + quantidade + ","
                + "\"preco\":289.90}]"                 // idem, dentro do item
                + "}";

        ProducerRecord<String, String> registro =
                new ProducerRecord<String, String>(topico, pedidoId, json);

        // Mesmo envelope binario que o servico-pedidos publica: os quatro
        // atributos exigidos pelo CloudEvents 1.0, mais o time.
        registro.headers().add("ce_specversion", "1.0".getBytes(UTF_8));
        registro.headers().add("ce_id", eventoId.getBytes(UTF_8));
        registro.headers().add("ce_source", "/loja/servico-pedidos".getBytes(UTF_8));
        registro.headers().add("ce_type", "loja.pedido.confirmado.v1".getBytes(UTF_8));
        registro.headers().add("ce_time", "2026-08-03T22:14:07.512Z".getBytes(UTF_8));

        template.send(registro);
        template.flush();
    }
}
