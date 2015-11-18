package org.jboss.ddoyle.drools.model;


public class DifferentExpirationSimpleEvent extends SimpleEvent {
	
	public DifferentExpirationSimpleEvent() {
		super();
	}
	
	public DifferentExpirationSimpleEvent(final String id) {
		super(id);
	}
	
	public DifferentExpirationSimpleEvent(final long timestamp) {
		super(timestamp);
		
	}
	
	public DifferentExpirationSimpleEvent(final String id, final long timestamp) {
		super(id, timestamp);
	}

}
