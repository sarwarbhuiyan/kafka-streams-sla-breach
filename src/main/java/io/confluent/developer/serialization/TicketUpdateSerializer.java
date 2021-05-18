package io.confluent.developer.serialization;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.serialization.Serializer;

import com.google.gson.Gson;

import io.confluent.developer.events.TicketUpdate;

public class TicketUpdateSerializer implements Serializer<TicketUpdate> {

	private Gson gson = new Gson();
	
	@Override
	public byte[] serialize(String topic, TicketUpdate data) {
		if (data == null) return null;
	    return gson.toJson(data).getBytes(StandardCharsets.UTF_8);
	}

}
