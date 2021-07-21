package com.od.helloclient;

import org.apache.log4j.Logger;
import org.springframework.http.MediaType;
import org.springframework.messaging.simp.stomp.StompFrameHandler;
import org.springframework.messaging.simp.stomp.StompHeaders;
import org.springframework.messaging.simp.stomp.StompSession;
import org.springframework.messaging.simp.stomp.StompSessionHandlerAdapter;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.socket.WebSocketHttpHeaders;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.messaging.WebSocketStompClient;
import org.springframework.web.socket.sockjs.client.SockJsClient;
import org.springframework.web.socket.sockjs.client.Transport;
import org.springframework.web.socket.sockjs.client.WebSocketTransport;
import org.springframework.web.socket.sockjs.frame.Jackson2SockJsMessageCodec;

import java.lang.reflect.Type;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Created by nick on 30/09/2015.
 */
public class HelloClient {

    private static Logger logger = Logger.getLogger(HelloClient.class);

    private final static WebSocketHttpHeaders headers = new WebSocketHttpHeaders();

    public ListenableFuture<StompSession> connect() {

        Transport webSocketTransport = new WebSocketTransport(new StandardWebSocketClient());
        List<Transport> transports = Collections.singletonList(webSocketTransport);

        SockJsClient sockJsClient = new SockJsClient(transports);
        sockJsClient.setMessageCodec(new Jackson2SockJsMessageCodec());

        WebSocketStompClient stompClient = new WebSocketStompClient(sockJsClient);

        String token = "eyJraWQiOiJKOEVZQmVDREtkMXlMY3hldDkxdHJsOUROdjZYXC9WenZGNE9iSmpvWUs5OD0iLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJlODhlNTU2MC0xYjc5LTRmZDEtYjk1Yy1jMWYwYjhjNmJhYzAiLCJldmVudF9pZCI6IjY3ZTYxOTVkLTBkMjctNDM2Yy1hMGNlLWI1MTUzOTM2MjRlOCIsInRva2VuX3VzZSI6ImFjY2VzcyIsInNjb3BlIjoiYXdzLmNvZ25pdG8uc2lnbmluLnVzZXIuYWRtaW4gb3BlbmlkIHByb2ZpbGUgZW1haWwiLCJhdXRoX3RpbWUiOjE2MjY0MjY0MjgsImlzcyI6Imh0dHBzOlwvXC9jb2duaXRvLWlkcC51cy1lYXN0LTEuYW1hem9uYXdzLmNvbVwvdXMtZWFzdC0xX3BxalZMZVpnSCIsImV4cCI6MTYyNjQzMDAyOCwiaWF0IjoxNjI2NDI2NDI5LCJ2ZXJzaW9uIjoyLCJqdGkiOiI3YzJjMzhhMy1hYzU1LTRhMGQtYTM1ZS0xOTYyNWYxNjMwNDEiLCJjbGllbnRfaWQiOiI0OGx1NGIxa2Y4MDU4cXAzcXA0bzcxa3F1YyIsInVzZXJuYW1lIjoiZTg4ZTU1NjAtMWI3OS00ZmQxLWI5NWMtYzFmMGI4YzZiYWMwIn0.Xg9BYfRPTjtzKZmg-6xO2pXoKXTT75Zm8E7_u1mkox9eOB8DY3xev2wwEqqNxztMwTamkLKLNMKnhKiqlbqwj5NWhifxCcvAUnBemIi7CEs3Ng0xUemoUIDdqs8dYjYXsBAl3joI9BpQJCrHWbtff36at-uYyWEjAHlqviune1Qp7lWFqF-SeFv0Hp_HgYJt-RIdCy3XwgutTTu_FKoA_veS-gTRmeOXTTbf_AoTDCBlkK4LSotTui3SpxvdoXjoEjOygrqA2gTxt4UY_WblrCOEBxN0X5RUvcglxJS9Hh2QGxbd5oj14RarUvhm39KfEaiTKafLT_N2ClPAJyCizQ";

        headers.set("Authorization","Bearer "+token);
        headers.set("accept-version","1.1,1.0");
        headers.set("heart-beat","110000,10000");

        String url = "wss://test.shiftmarkets.com/websocket/v2";
        return stompClient.connect(url, headers, new MyHandler());
    }

    public void subscribeGreetings(StompSession stompSession) throws ExecutionException, InterruptedException {
        StompHeaders stompHeaders = new StompHeaders();
        stompHeaders.setDestination("/user/v1/market-data/BTCUSD");
        stompHeaders.setId("sub-1");
        Date date = new Date();
        String timestamp = new Timestamp(date.getTime()).getTime()+"";
        stompHeaders.set("X-Deltix-Nonce",timestamp);
        stompSession.subscribe(stompHeaders, new StompFrameHandler() {

            public Type getPayloadType(StompHeaders stompHeaders) {
                return byte[].class;
            }

            public void handleFrame(StompHeaders stompHeaders, Object o) {
                logger.info("Received Messages " + new String((byte[]) o));
            }
        });
    }

    public void sendHello(StompSession stompSession) {
        String jsonHello = "{ \"name\" : \"Nick\" }";
        stompSession.send("/app/hello", jsonHello.getBytes());
    }

    private class MyHandler extends StompSessionHandlerAdapter {
        public void afterConnected(StompSession stompSession, StompHeaders stompHeaders) {
            logger.info("Now connected");
        }
    }
    
    public static void main(String[] args) throws Exception {
        HelloClient helloClient = new HelloClient();

        ListenableFuture<StompSession> f = helloClient.connect();
        StompSession stompSession = f.get();

        logger.info("Subscribing to greeting topic using session " + stompSession);
        helloClient.subscribeGreetings(stompSession);

        //logger.info("Sending hello message" + stompSession);
        //helloClient.sendHello(stompSession);
        Thread.sleep(60000);
    }
    
}
