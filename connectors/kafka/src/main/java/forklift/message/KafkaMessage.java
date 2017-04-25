package forklift.message;

import forklift.connectors.ConnectorException;
import forklift.connectors.ForkliftMessage;
import forklift.consumer.parser.KeyValueParser;
import forklift.controller.KafkaController;
import forklift.message.SerializedMessage;
import forklift.producers.KafkaForkliftProducer;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class KafkaMessage extends ForkliftMessage implements SerializedMessage {
    private final KafkaController controller;
    private final ConsumerRecord<?, ?> consumerRecord;
    private final Object deserializedValue;

    public KafkaMessage(KafkaController controller, ConsumerRecord<?, ?> consumerRecord) {
        this.controller = controller;
        this.consumerRecord = consumerRecord;
        this.deserializedValue = controller.getValueDeserializer().deserialize(consumerRecord.topic(), (byte[]) consumerRecord.value());

        createMessage();
    }

    public ConsumerRecord<?, ?> getConsumerRecord() {
        return this.consumerRecord;
    }

    @Override
    public boolean acknowledge() throws ConnectorException {
        try {
            return controller.acknowledge(consumerRecord);
        } catch (InterruptedException e) {
            throw new ConnectorException("Error acknowledging message");
        }
    }

    @Override
    public String getId() {
        return consumerRecord.topic() + "-" + consumerRecord.partition() + "-" + consumerRecord.offset();
    }

    public byte[] getSerializedBytes() {
        return (byte[]) consumerRecord.value();
    }

    /**
     * <strong>WARNING:</strong> Called from constructor
     */
    private final void createMessage() {
        String message = parseRecord();
        if (message != null) {
            setMsg(message);
        }
        else{
            this.setFlagged(true);
            this.setWarning("Unable to parse message for topic: " + consumerRecord.topic() +
                            " with value: " + deserializedValue);
        }
    }

    private final String parseRecord(){
        Object value = null;
        if (deserializedValue instanceof GenericRecord) {
            GenericRecord genericRecord = (GenericRecord) deserializedValue;
            Object properties = genericRecord.get(KafkaForkliftProducer.SCHEMA_FIELD_NAME_PROPERTIES);
            if (properties != null) {
                this.setProperties(KeyValueParser.parse(properties.toString()));
            }
            value = genericRecord.get(KafkaForkliftProducer.SCHEMA_FIELD_NAME_VALUE);
            //If the value is null, this is most likely an avro object
            if (value == null) {
                String jsonValue = genericRecord.toString();
                value = jsonValue != null && jsonValue.startsWith("{") ? jsonValue : null;
            }
        }
        return value == null?null:value.toString();
    }
}
