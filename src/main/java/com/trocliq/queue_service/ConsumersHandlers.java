/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.trocliq.queue_service;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mail.MailClient;
import io.vertx.ext.mail.MailMessage;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import java.sql.Timestamp;

/**
 *
 * @author dipo
 */
public class ConsumersHandlers {

    private final WebClient webClient;
    private final MailClient mailClient;

    public ConsumersHandlers(WebClient webClient, MailClient mailClient) {
        this.webClient = webClient;
        this.mailClient = mailClient;
    }

    public Future<HttpResponse> defaultHandler(KafkaConsumerRecord<Object, Object> record) {
        Future<HttpResponse> future = Future.future();

        System.out.println("Processing key=" + record.key() + ",value=" + record.value()
                + ",partition=" + record.partition() + ",offset=" + record.offset());
        JsonObject message = (JsonObject) record.value();
        String callbackUrl = message.getString("url");
        String apiTestUrl = message.getString("api-test-url");

//        webClient
//                .postAbs(apiTestUrl)
//                .sendJsonObject(message, ar -> {
//                    if (ar.succeeded()) {
//                        System.out.println("passed");
//                        System.out.println(ar.result().statusCode());
//                        System.out.println(ar.result().statusMessage());
//                        System.out.println(ar.result().bodyAsString());
//                        webClient.close();
//
//                        webClient
//                                .postAbs(callbackUrl)
//                                .sendJsonObject(ar.result().bodyAsJsonObject().put("forensic", "passed"), l -> {
//                                    webClient.close();
//                                });
//
//                        future.complete(ar.result());
//                    }
//                    if (ar.failed()) {
//                        ar.cause().printStackTrace();
//                        webClient
//                                .postAbs(callbackUrl)
//                                .sendJsonObject(new JsonObject().put("forensic", ar.cause().toString()), l -> {
//                                    webClient.close();
//                                });
//                        future.fail(ar.cause());
//                    }
//                });
        MailMessage emailMessage = new MailMessage();
        emailMessage.setFrom("olabosindeoladipo@gmail.com (Olabosinde Oladipo)");
        emailMessage.setTo("drealdondippino@yahoo.com");
        emailMessage.setCc("Olabosinde Oladipo <olabosindeoladipo@gmail.com>");
        emailMessage.setText("this is the plain message text");
//        emailMessage.setHtml("this is html text <a href=\"http://vertx.io\">vertx.io</a>");

        mailClient.sendMail(emailMessage, result -> {
            if (result.succeeded()) {
                System.out.println(result.result());
            } else {
                result.cause().printStackTrace();
            }
        });
        return future;
    }

    public void retry1Handler(KafkaConsumerRecord hndlr) {

    }

    public void retry2Handler(KafkaConsumerRecord hndlr) {

    }

    public void retry3Handler(KafkaConsumerRecord hndlr) {

    }

    public void retry4Handler(KafkaConsumerRecord hndlr) {

    }

    public void retry5Handler(KafkaConsumerRecord hndlr) {

    }

    private long timeStamp() {
        return new Timestamp(System.currentTimeMillis()).getTime();
    }

}
