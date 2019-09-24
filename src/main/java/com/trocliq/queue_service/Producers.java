/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.trocliq.queue_service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.OffsetAndMetadata;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author dipo
 */
public class Producers {

    /**
     *
     * @param producer
     * @param topic
     * @param key
     * @param value
     * @param timeStamp
     * @param partition
     * @param resultHandler
     */
    protected void writeToKafka(KafkaProducer producer, String topic, String key, JsonObject value, long timeStamp, int partition, Handler<AsyncResult<KafkaProducerRecord>> resultHandler) {

        KafkaProducerRecord<String, JsonObject> record = KafkaProducerRecord.create(topic, key, value, timeStamp, partition);

        producer.write(record, done -> {
            resultHandler.handle(Future.succeededFuture(record));
        }).exceptionHandler((error) -> {
            resultHandler.handle(Future.failedFuture("Could not write to kafka"));
        });
    }

    /**
     *
     * @param producer
     * @param topic
     * @param key
     * @param value
     * @param timeStamp
     * @param partition
     * @param resultHandler
     */
    protected void sendToDLQ(KafkaProducer producer, String topic, String key, JsonObject value, long timeStamp, int partition, KafkaConsumer consumer, KafkaConsumerRecord record) {
        writeToKafka(producer, topic, key, value, timeStamp, partition, hr -> {
            TopicPartition tp = new TopicPartition(record.topic(), record.partition());
            OffsetAndMetadata mt = new OffsetAndMetadata(record.offset(), "DLQ");
            Map<TopicPartition, OffsetAndMetadata> dt = new HashMap<>();
            dt.put(tp, mt);
            consumer.commit(dt, calbk -> {
            });
        });
    }

    protected long timeStamp() {
        return new Timestamp(System.currentTimeMillis()).getTime();
    }
}
