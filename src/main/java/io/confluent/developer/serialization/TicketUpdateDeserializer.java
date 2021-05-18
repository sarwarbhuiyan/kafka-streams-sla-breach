package io.confluent.developer.serialization;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.serialization.Deserializer;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import io.confluent.developer.events.TicketUpdate;

public class TicketUpdateDeserializer implements Deserializer<TicketUpdate> {

	private Gson gson =
		      new GsonBuilder()
		          .setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE)
		          .create();
	
	@Override
	public TicketUpdate deserialize(String topic, byte[] data) {
		if (data == null) return null; 
	    return gson.fromJson(
	      new String(data, StandardCharsets.UTF_8), TicketUpdate.class); 
	}

	
}
