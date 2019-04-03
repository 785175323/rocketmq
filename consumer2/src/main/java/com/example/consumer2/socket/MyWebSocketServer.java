package com.example.consumer2.socket;

import java.io.IOException;
import java.util.concurrent.CopyOnWriteArraySet;

import javax.websocket.OnClose;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;

import org.springframework.stereotype.Component;

@ServerEndpoint("/default2")
@Component
public class MyWebSocketServer {

    public static CopyOnWriteArraySet<MyWebSocketServer> webSocketSet = new CopyOnWriteArraySet<MyWebSocketServer>();
    private Session session;

    @OnOpen
    public void onOpen(Session session) {
        this.session = session;
        webSocketSet.add(this);
    }

    @OnClose
    public void onClose() {
        webSocketSet.remove(this);
    }


    public void sendMessage(String message) throws IOException {
        this.session.getBasicRemote().sendText(message);
    }

}

