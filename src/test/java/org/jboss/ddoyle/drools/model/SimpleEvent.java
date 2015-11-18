package org.jboss.ddoyle.drools.model;

import java.util.Date;
import java.util.UUID;

public class SimpleEvent {
	
	
	private final long timestamp;
	
	private final String id;
	
	public SimpleEvent() {
		this(UUID.randomUUID().toString(), new Date().getTime());
	}
	
	public SimpleEvent(final String id) {
		this(id, new Date().getTime());
	}
	
	public SimpleEvent(final long timestamp) {
		this(UUID.randomUUID().toString(), timestamp);
		
	}
	
	public SimpleEvent(final String id, final long timestamp) {
		this.id = id;
		this.timestamp = timestamp;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public String getId() {
		return id;
	}
	

}
