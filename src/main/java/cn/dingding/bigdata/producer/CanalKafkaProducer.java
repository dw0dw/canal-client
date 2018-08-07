package cn.dingding.bigdata.producer;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * kafka producer 主操作类
 *
 * @author machengyuan 2018-6-11 下午05:30:49
 * @version 1.0.0
 */
public class CanalKafkaProducer {

    private static final Logger logger = LoggerFactory.getLogger(CanalKafkaProducer.class);

    private Producer<String, String> producer;
    private static KafkaProperties.Topic topic = null;
    public void init(KafkaProperties kafkaProperties) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", kafkaProperties.getServers());
        properties.put("acks", "all");
        properties.put("retries", kafkaProperties.getRetries());
        properties.put("batch.size", kafkaProperties.getBatchSize());
        properties.put("linger.ms", kafkaProperties.getLingerMs());
        properties.put("buffer.memory", kafkaProperties.getBufferMemory());
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());
        topic = new KafkaProperties.Topic();
        topic.setTopic(kafkaProperties.getTopic());
        producer = new KafkaProducer<String, String>(properties);
    }

    public void stop() {
        try {
            logger.info("## stop the kafka producer");
            producer.close();
        } catch (Throwable e) {
            logger.warn("##something goes wrong when stopping kafka producer:", e);
        } finally {
            logger.info("## kafka producer is down.");
        }
    }

    public void send(KafkaProperties.Topic topic, CanalEntry.Entry entry, JSONObject jsonObject) {
        boolean valid = false;
        if (entry.getEntryType() != CanalEntry.EntryType.TRANSACTIONBEGIN
                    && entry.getEntryType() != CanalEntry.EntryType.TRANSACTIONEND) {
                    valid = true;
        }
//        if (!valid) {
//            return;
//        }

        ProducerRecord<String, String> record;
        if (topic.getPartition() != null) {
            record = new ProducerRecord<String, String>(topic.getTopic(), topic.getPartition(), null, jsonObject.toJSONString());
        } else {
            record = new ProducerRecord<String, String>(topic.getTopic(), jsonObject.toJSONString());
        }
        producer.send(record);
        logger.debug("send message to kafka topic: {} \n {}", topic, jsonObject);
    }

    public void send(int partition, CanalEntry.Entry entry, JSONObject jsonObject) {
        boolean valid = false;
        if (entry.getEntryType() != CanalEntry.EntryType.TRANSACTIONBEGIN
                && entry.getEntryType() != CanalEntry.EntryType.TRANSACTIONEND) {
            valid = true;
        }
        ProducerRecord<String, String> record;
        if (partition <0) {
            record = new ProducerRecord<String, String>(topic.getTopic(), topic.getPartition(), null, jsonObject.toJSONString());
        } else {
            record = new ProducerRecord<String, String>(topic.getTopic(), jsonObject.toJSONString());
        }
        producer.send(record);
        logger.debug("send message to kafka topic: {} \n {}", topic, jsonObject);
    }
}
