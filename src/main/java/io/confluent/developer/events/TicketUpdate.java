package io.confluent.developer.events;

public class TicketUpdate {

	enum TicketStatus {
		CREATED,
		ASSIGNED,
		UPDATED,
		SOLVED,
		CLOSED
	}
	
	private String user;
	
	private String id;
	
	private TicketStatus ticketStatus = TicketStatus.CREATED;
	
	private String comment;

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public TicketStatus getTicketStatus() {
		return ticketStatus;
	}

	public void setTicketStatus(TicketStatus ticketStatus) {
		this.ticketStatus = ticketStatus;
	}

	public String getComment() {
		return comment;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}

	
}
