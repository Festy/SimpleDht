package edu.buffalo.cse.cse486586.simpledht;

import java.io.Serializable;

/**
 * Created by utsavpatel on 3/28/15.
 */

public class Message implements Serializable{
    public enum TYPE{
        JOIN, ADD, DELETE_ONE, DELETE_ALL, GET_ONE, GET_ALL;
    }
//    private String sender;
    private String senderPort;
    private String message;
    private String remotePort;
    private TYPE type;

//    public void setSender(String sender) {
//        this.sender = sender;
//    }

    public void setMessage(String message) {
        this.message = message;
    }

    public void setSenderPort(String senderPort) {
        this.senderPort = senderPort;
    }

//    public String getSender() {
//        return sender;
//    }

    public String getMessage() {
        return message;
    }

    public String getSenderPort() {
        return senderPort;
    }

    public void setRemotePort(String remotePort) {
        this.remotePort = remotePort;
    }

    public String getRemotePort() {
        return remotePort;
    }

    public void setType(TYPE type) {
        this.type = type;
    }

    public TYPE getType() {
        return type;
    }
}
