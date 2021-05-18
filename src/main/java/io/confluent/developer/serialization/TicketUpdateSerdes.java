package io.confluent.developer.serialization;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import io.confluent.developer.events.TicketUpdate;

public class TicketUpdateSerdes implements Serde<TicketUpdate> {

	@Override
	public Serializer<TicketUpdate> serializer() {
		return new TicketUpdateSerializer();
	}

	@Override
	public Deserializer<TicketUpdate> deserializer() {
		return new TicketUpdateDeserializer();
	}

}
