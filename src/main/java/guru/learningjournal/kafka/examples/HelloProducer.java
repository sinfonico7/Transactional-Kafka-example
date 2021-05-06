package guru.learningjournal.kafka.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

public class HelloProducer {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {

        logger.info("Creating Kafka Producer...");
        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfigs.applicationID);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,AppConfigs.transaction_id);
        KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);
        //first step to implment a trasaction method
        producer.initTransactions();

        logger.info("Beginning the first transaction");
        producer.beginTransaction();
        try{
            logger.info("CASE WITH ATOMIC TRANSACTION SCENARIO");
            logger.info("FIRST TRANSACTION => Start sending messages...");
        for (int i = 1; i <= AppConfigs.numEvents; i++) {
            producer.send(new ProducerRecord<>(AppConfigs.topicName1, i, "FIRST TRANSACTION => Simple Message-" + i));
            producer.send(new ProducerRecord<>(AppConfigs.topicName2, i, "FIRST TRANSACTION => Simple Message-" + i));
        }
        producer.commitTransaction();
            logger.info("FIRST TRANSACTION => => All ok!");
        }catch(Exception e){
            producer.abortTransaction();
            producer.close();
            logger.info("FIRST TRANSACTION => Error on the FIRST transaction.");
            throw new RuntimeException(e);
        }
        logger.info("FIRST TRANSACTION ENDS => - Closing Kafka Producer.");

        logger.info("Beginning with the second transaction");
        producer.beginTransaction();
        try{
            logger.info("CASE WITH ROLLBACK SCENARIO");
            logger.info("SECOND TRANSACTION => sending messages...");
            for (int i = 1; i <= AppConfigs.numEvents; i++) {
                producer.send(new ProducerRecord<>(AppConfigs.topicName1, i, "SECOND TRANSACTION => Simple Message-" + i));
                producer.send(new ProducerRecord<>(AppConfigs.topicName2, i, "SECOND TRANSACTION => Simple Message-" + i));
            }
            producer.abortTransaction();
            logger.info("SECOND TRANSACTION => Aborting the transaction.");
        }catch(Exception e){
            producer.abortTransaction();
            producer.close();
            logger.info("SECOND TRANSACTION => Error on transaction.");
            throw new RuntimeException(e);
        }
        logger.info("Finished Second Transaction - Closing Kafka Producer.");




        producer.close();

    }
}
