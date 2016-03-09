package org.greysguez.rabbitmq.dsl;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONObject;

import com.google.gson.Gson;

public class Message implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private String type;
	private String correlationId;
	private String payload;

    private final Map<String, Serializable> data = new HashMap<String, Serializable>();

    public Message(String type, String correlationId) {
    	this.type = type;
		this.correlationId = correlationId;
    }
    
    public Message(String type) {
    	this.type = type;
		this.correlationId = java.util.UUID.randomUUID().toString();
    }
    
    public void setPayload(String json) {
		this.payload = json;
	}
    
    public String getPayload() {
		return payload;
	}
    
    public String getType() {
		return type;
	}
    
    public String getCorrelationId() {
		return correlationId;
	}
    
    public Serializable getData(String key) {
		return data.get(key);
	}

	public void putData(String key, Serializable value) {
		data.put(key, value);
	}

	public void clearData() {
		data.clear();
	}
    
    public Map<String, Serializable> getData() {
		return data;
	}
    
    public static Message fromJson(String message){
    	return new Gson().fromJson(message, Message.class);
    }
    
    public String toJson(){
    	return new JSONObject(this).toString();
    }
}
