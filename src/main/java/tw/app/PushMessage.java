package tw.app;

import java.time.Instant;

public class PushMessage {

	String sender, topic, text;
	boolean urgent;
	Instant sendTime;

	public void setSender (String sender) {
		this.sender = sender;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public void setText(String text) {
		this.text = text;
	}

	public void setUrgent(boolean urgent) {
		this.urgent = urgent;
	}

	public void setSendTime(Instant sendTime){
		this.sendTime = sendTime;
	}

	public String getSender(){
		return this.sender;
	}

	public String getTopic(){
		return this.topic;
	}

	public String getText(){
		return this.text;
	}

	public boolean getUrgent(){
		return this.urgent;
	}

	public Instant getSendTime(){
		return this.sendTime;
	}
}