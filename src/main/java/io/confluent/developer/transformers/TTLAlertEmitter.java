package io.confluent.developer.transformers;

import java.time.Duration;
import java.util.function.Function;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;


public class TTLAlertEmitter<K, V, R> implements Transformer<K, V, R> {

	  private final Duration maxAge;
	  private final Duration scanFrequency;
	  private final String purgeStoreName;
	  private ProcessorContext context;
	  private KeyValueStore<K, Long> stateStore;
	  private Function<K, String> alertFunction;

	  public TTLAlertEmitter(final Duration maxAge, 
			  final Duration scanFrequency,
	      final String stateStoreName, Function<K, String> alertFunction) {
		 
	    this.maxAge = maxAge;
	    this.scanFrequency = scanFrequency;
	    this.purgeStoreName = stateStoreName;
	    this.alertFunction = alertFunction;
	  }

	  @Override
	  public void init(ProcessorContext context) {
	    this.context = context;
	    this.stateStore = (KeyValueStore<K, Long>) context.getStateStore(purgeStoreName);
	    // This is where the magic happens. This causes Streams to invoke the Punctuator
	    // on an interval, using stream time. That is, time is only advanced by the record
	    // timestamps
	    // that Streams observes. This has several advantages over wall-clock time for this
	    // application:
	    //
	    // It'll produce the exact same sequence of updates given the same sequence of data.
	    // This seems nice, since the purpose is to modify the data stream itself, you want to
	    // have a clear understanding of when stuff is going to get deleted. For example, if something
	    // breaks down upstream for this topic, and it stops getting new data for a while, wall
	    // clock time would just keep deleting data on schedule, whereas stream time will wait for
	    // new updates to come in.
	    //
	    // You can change to wall clock time here if that is what is needed
	    context.schedule(scanFrequency, PunctuationType.WALL_CLOCK_TIME, timestamp -> {
	      final long cutoff = timestamp - maxAge.toMillis();

	      // scan over all the keys in this partition's store
	      // this can be optimized, but just keeping it simple.
	      // this might take a while, so the Streams timeouts should take this into account
	      try (final KeyValueIterator<K, Long> all = stateStore.all()) {
	        while (all.hasNext()) {
	          final KeyValue<K, Long> record = all.next();
	          if (record.value != null && record.value < cutoff) {
	            System.out.println("Generating alert");
	            // if a record's last update was older than our cutoff key
	            context.forward(record.key, alertFunction.apply(record.key));
	            // remove key from store so as not to alert again
	            stateStore.delete(record.key);
	          }
	        }
	      }
	    });
	  }

	  @Override
	  public R transform(K key, V value) {
		
		System.out.println("TRANSFORM"+key+" "+value);
	    Long lastUpdated = stateStore.get(key);
	    
	    if (lastUpdated == null) {
	      System.out.println("Never seen before key="+key);
	      //never seen this before, let's store it
	      stateStore.put(key, context.timestamp());
	    } else {
	      System.out.println("Seen key before, therefore deleting key="+key);
	      stateStore.put(key, null); //clearing what could have been an alert
	    }
	    return null; // no need to return anything here. the punctuator will emit the tombstones
	                 // when necessary
	  }

	  @Override
	  public void close() {


	  }



}