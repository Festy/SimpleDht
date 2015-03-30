package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentValues;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Created by utsavpatel on 3/28/15.
 */

public class Message implements Serializable{
    public enum TYPE{
        JOIN, ADD, DELETE_ONE, DELETE_ALL, GET_ONE, GET_ALL, UPDATE_LINKS, REPLY_ONE, REPLY_ALL ;
    }
//    private String sender;
    private String senderPort;
    private String message;
    private String remotePort;
    private TYPE type;
    private String pre;
    private String succ;
    private String keyHash;
    private String key;
    private String value;
    private HashMap<String,String> result;
//    private Uri uri;

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

    public void setPre(String pre) {
        this.pre = pre;
    }

    public void setSucc(String succ) {
        this.succ = succ;
    }

    public String getPre() {
        return pre;
    }

    public String getSucc() {
        return succ;
    }

    public void setKeyHash(String keyHash) {
        this.keyHash = keyHash;
    }

    public String getKeyHash() {
        return keyHash;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public void setResult(HashMap<String, String> result) {
        this.result = result;
    }

    public HashMap<String, String> getResult() {
        return result;
    }
    //    public void setUri(Uri uri) {
//        this.uri = uri;
//    }
//
//    public Uri getUri() {
//        return uri;
//    }

    public ContentValues getContentValue(){
        ContentValues cv = new ContentValues();
        cv.put("key",getKey());
        cv.put("value",getValue());
        return cv;
    }
}
