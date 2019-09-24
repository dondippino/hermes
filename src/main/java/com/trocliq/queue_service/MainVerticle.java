package com.trocliq.queue_service;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mail.MailClient;
import io.vertx.ext.mail.MailConfig;
import io.vertx.ext.mail.StartTLSOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class MainVerticle extends AbstractVerticle {

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        WebClientOptions options = new WebClientOptions();
        options.setKeepAlive(false);
        WebClient webClient = WebClient.create(vertx);
        //===============================================
        MailConfig mailConfig = new MailConfig();
        mailConfig.setHostname("smtp.gmail.com");
        mailConfig.setPort(587);
        mailConfig.setStarttls(StartTLSOptions.REQUIRED);
        mailConfig.setUsername("olabosindeoladipo@gmail.com");
        mailConfig.setPassword("#Seanhaynes1986");
        MailClient mailClient = MailClient.createNonShared(vertx, mailConfig);
        //===============================================
        Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create());
        //===============================================
        //========= initialize Producer =================
        Map<String, String> config2 = new HashMap<>();
        config2.put("bootstrap.servers", "172.17.0.1:9092");
        config2.put("key.serializer", "io.vertx.kafka.client.serialization.JsonObjectSerializer");
        config2.put("value.serializer", "io.vertx.kafka.client.serialization.JsonObjectSerializer");
        config2.put("acks", "1");

        // use producer for interacting with Apache Kafka
        KafkaProducer<String, JsonObject> producer = KafkaProducer.create(vertx, config2);
        //========= End initialize Producer ==============

        //========= initialize Consumer ==================
        Map<String, String> config = new HashMap<>();
        config.put("bootstrap.servers", "172.17.0.1:9092");
        config.put("key.deserializer", "io.vertx.kafka.client.serialization.JsonObjectDeserializer");
        config.put("value.deserializer", "io.vertx.kafka.client.serialization.JsonObjectDeserializer");
        config.put("group.id", "my_group");
        config.put("auto.offset.reset", "earliest");
        config.put("enable.auto.commit", "false");

        // use consumer for interacting with Apache Kafka
        KafkaConsumer consumer = KafkaConsumer.create(vertx, config).subscribe(new HashSet<>(Arrays.asList("topic1", "topic2", "topic3"))).handler(new ConsumersHandlers(webClient, mailClient)::defaultHandler);
        KafkaConsumer consumerDLQ = KafkaConsumer.create(vertx, config).subscribe("dlq");
        KafkaConsumer consumerRetry1 = KafkaConsumer.create(vertx, config).subscribe("retry_send_1").handler(new ConsumersHandlers(webClient, mailClient)::retry1Handler);
        KafkaConsumer consumerRetry2 = KafkaConsumer.create(vertx, config).subscribe("retry_send_2").handler(new ConsumersHandlers(webClient, mailClient)::retry2Handler);
        KafkaConsumer consumerRetry3 = KafkaConsumer.create(vertx, config).subscribe("retry_send_3").handler(new ConsumersHandlers(webClient, mailClient)::retry3Handler);
        KafkaConsumer consumerRetry4 = KafkaConsumer.create(vertx, config).subscribe("retry_send_4").handler(new ConsumersHandlers(webClient, mailClient)::retry4Handler);
        KafkaConsumer consumerRetry5 = KafkaConsumer.create(vertx, config).subscribe("retry_send_5").handler(new ConsumersHandlers(webClient, mailClient)::retry5Handler);

        // subscribe to several topics
        //===========================================================
        //===========================================================
        //========= End initialize Consumer ==============
        //==============================================
        router.get(
                "/").handler(routingContext -> {
                    routingContext.response().end("Hello from Vert.x!");
                }
                );

        router.get("/:topic/:partition/:offset").handler(routingContext -> {
            String topic = routingContext.request().getParam("topic");
            String partition = routingContext.request().getParam("partition");
            String offset = routingContext.request().getParam("offset");
            TopicPartition topicPartition = new TopicPartition()
                    .setTopic(topic)
                    .setPartition(Integer.parseInt(partition));

// seek to a specific offset
            consumer.seek(topicPartition, Integer.parseInt(offset), done -> {
                routingContext.response().setStatusCode(200).end("{}");
            });
        });
        router.post("/:topic").handler(routingContext -> {

            String topic = routingContext.request().getParam("topic");

            System.out.println(topic);
            Date date = new Date();

            long time = date.getTime();
            System.out.println("Time in Milliseconds: " + time);

            Timestamp ts = new Timestamp(time);
            System.out.println("Current Time Stamp: " + ts);
            JsonObject body = routingContext.getBodyAsJson();
            KafkaProducerRecord<String, JsonObject> record
                    = KafkaProducerRecord.create(topic, null, body, 0);
            body.put("timestamp", ts.toString());

            new Producers().writeToKafka(producer, topic, null, body, new Producers().timeStamp(), 0, hr -> {
                if (hr.succeeded()) {
                    System.out.println("Sent");
                    routingContext.response().end(record.value().encode());

                }
                if (hr.failed()) {
                    System.out.println("failed");
                    hr.cause().printStackTrace();
                    routingContext.response().end(record.value().encode());
                }
            });

        }
        );

        //=================================================
        //==============================================
        vertx.createHttpServer()
                .requestHandler(router).listen(8888, http -> {
            if (http.succeeded()) {
                startFuture.complete();
                System.out.println("HTTP server started on port 8888");
            } else {
                startFuture.fail(http.cause());
            }
        }
        );

    }

}
