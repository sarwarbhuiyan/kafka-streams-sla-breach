package io.confluent.developer;

import static java.lang.Integer.parseInt;
import static java.lang.Short.parseShort;
import static java.util.stream.Collectors.toMap;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.REPLICATION_FACTOR_CONFIG;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import io.confluent.developer.events.TicketUpdate;
import io.confluent.developer.serialization.TicketUpdateSerdes;
import io.confluent.developer.transformers.TTLAlertEmitter;

public class SLABreachApplication {

	private static final String INPUT_TICKET_UPDATES_TOPIC_NAME_CONFIG = "input.ticket-updates.topic.name";
	private static Logger log = LoggerFactory.getLogger(SLABreachApplication.class);

	// region buildStreamsProperties
	protected Properties buildStreamsProperties(Properties envProps) {
		Properties config = new Properties();
		config.putAll(envProps);

		config.put(APPLICATION_ID_CONFIG, envProps.getProperty("application.id"));
		config.put(BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));
		//config.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		//config.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		// config.put(SCHEMA_REGISTRY_URL_CONFIG,
		// envProps.getProperty("schema.registry.url"));

		config.put(REPLICATION_FACTOR_CONFIG, envProps.getProperty("default.topic.replication.factor"));
		config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, envProps.getProperty("offset.reset.policy"));

		//config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

		return config;
	}
	// endregion

	protected Properties loadEnvProperties(String fileName) throws IOException {
		final Properties envProps = new Properties();
		final FileInputStream input = new FileInputStream(fileName);
		envProps.load(input);
		input.close();

		// These two settings are only required in this contrived example so that the
		// streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0);
		// streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
		return envProps;
	}

	// region buildTopology
	private Topology buildTopology(StreamsBuilder builder, Properties envProps) {
		final TicketUpdateSerdes ticketUpdateSerdes = new TicketUpdateSerdes();
		final String ticketUpdatesTopic = envProps.getProperty(INPUT_TICKET_UPDATES_TOPIC_NAME_CONFIG);
//		KTable<String,TicketUpdate> ticketUpdatesTable = builder.table(ticketUpdatesTopic);
		
		KStream<String, TicketUpdate> ticketUpdatesStream = builder.stream(ticketUpdatesTopic,
				Consumed.with(Serdes.String(), ticketUpdateSerdes));
//		KStream<String, TicketUpdate> ticketUpdatesStream = ticketUpdatesTable.toStream();
		KTable<String, TicketUpdate> latestTicketUpdates = ticketUpdatesStream.toTable();
				latestTicketUpdates.toStream().to("test-tickets-table");
		
		KTable<Windowed<String>, Integer> ticketUpdatesCountTable = ticketUpdatesStream.groupByKey()
				.windowedBy(SessionWindows.with(Duration.ofMinutes(1)).grace(Duration.ofSeconds(5)))
				// do simple count of how many updates to the ticket has happened
				.aggregate(
						() -> 0, 
						(key, value, aggregate) -> {
							// log.debug("aggregate() - key={} value={} aggregate={}", key, value,
							// aggregate);
							return aggregate + 1;
						},

						(aggKey, aggOne, aggTwo) -> {
							// log.debug("merge() - key={} aggOne={} aggTwo={}", aggKey, aggOne, aggTwo);
							return aggOne + aggTwo;
						},
						Materialized.<String, Integer, SessionStore<Bytes, byte[]>>as("latest-ticket-counts")
								.withKeySerde(Serdes.String()).withValueSerde(Serdes.Integer()))
				.suppress(Suppressed.untilWindowCloses(BufferConfig.unbounded()));
		
	

		final Duration MAX_AGE = Duration
				.ofMinutes(Integer.parseInt(envProps.getProperty("expiry.store.cutoff.minutes")));
		final Duration SCAN_FREQUENCY = Duration
				.ofSeconds(Integer.parseInt(envProps.getProperty("expiry.store.scan.seconds")));
		final String STATE_STORE_NAME = envProps.getProperty("expiry.store.name");
		// adding a custom state store for the TTL transformer which has a key of type
		// string, and a
		// value of type long
		// which represents the timestamp
		final StoreBuilder<KeyValueStore<String, Long>> storeBuilder = Stores
				.keyValueStoreBuilder(Stores.persistentKeyValueStore(STATE_STORE_NAME), Serdes.String(), Serdes.Long());
		builder.addStateStore(storeBuilder);

		String alertsTopic = envProps.getProperty("output.sla-alerts.topic.name", "sla-alerts");

		ticketUpdatesCountTable.toStream()
		   .peek((k,v) -> log.info("BEFORE FILTER "+k+" "+v))
		   .filter((key,value) -> value!=null && value == 1)
		   .map((k,v) -> KeyValue.pair(k.key(),"Missing Event within timeout period"))
		   .to(alertsTopic, Produced.with(Serdes.String(), Serdes.String()));
		
		ticketUpdatesStream
				.transform(() -> new TTLAlertEmitter(MAX_AGE, SCAN_FREQUENCY, STATE_STORE_NAME, s -> s+" has breached sla"), STATE_STORE_NAME)
				.to(alertsTopic, Produced.with(Serdes.String(), Serdes.String()));

		return builder.build();
	}
	// endregion

	private void createTopics(Properties envProps) {
		Map<String, Object> config = new HashMap<>();

		config.put("bootstrap.servers", envProps.getProperty("bootstrap.servers"));
		AdminClient client = AdminClient.create(config);

		List<NewTopic> topics = new ArrayList<>();

		topics.add(new NewTopic(envProps.getProperty(INPUT_TICKET_UPDATES_TOPIC_NAME_CONFIG),
				parseInt(envProps.getProperty("input.ticket-updates.topic.partitions")),
				parseShort(envProps.getProperty("input.ticket-updates.topic.replication.factor"))));

		topics.add(new NewTopic(envProps.getProperty("output.sla-alerts.topic.name"),
				parseInt(envProps.getProperty("output.sla-alerts.topic.partitions")),
				parseShort(envProps.getProperty("output.sla-alerts.topic.replication.factor"))));

		client.createTopics(topics);
		client.close();

	}

	public void run(String fileName) {

		Properties envProps = null;
		try {
			envProps = this.loadEnvProperties(fileName);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			System.exit(1);
		}
		Properties streamProps = this.buildStreamsProperties(envProps);
		Topology topology = this.buildTopology(new StreamsBuilder(), envProps);
		log.info("TOPOLOGY:\n"+topology.describe());
		this.createTopics(envProps);

		final KafkaStreams streams = new KafkaStreams(topology, streamProps);
		final CountDownLatch latch = new CountDownLatch(1);

		// Attach shutdown handler to catch Control-C.
		Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
			@Override
			public void run() {
				streams.close(Duration.ofSeconds(5));
				latch.countDown();
			}
		});

		try {
			streams.cleanUp();
			streams.start();
			latch.await();
		} catch (Throwable e) {
			System.exit(1);
		}
		System.exit(0);
	}

	public static void main(String[] args) {

	    if (args.length < 1) {
	      throw new IllegalArgumentException(
	          "This program takes one argument: the path to an environment configuration file.");
	    }
		new SLABreachApplication().run(args[0]);

	}
}
