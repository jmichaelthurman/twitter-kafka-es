import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterConsumer {



    public static void run(String consumerKey, String consumerSecret, String token, String secret) throws IOException {

        Logger logger = LoggerFactory.getLogger(TwitterConsumer.class);

        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
        BlockingQueue<String> eventQueue = new LinkedBlockingQueue<String>(100);

        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
// Optional: set up some followings and track terms
        List<Long> followings = Lists.newArrayList(1234L, 566788L);
        List<String> terms = Lists.newArrayList("Afghanistan");
        hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);

        try {
            Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

            ClientBuilder builder = new ClientBuilder()
                    .name("Hosebird-Client-01")                              // optional: mainly for the logs
                    .hosts(hosebirdHosts)
                    .authentication(hosebirdAuth)
                    .endpoint(hosebirdEndpoint)
                    .processor(new StringDelimitedProcessor(msgQueue));
                    //.eventMessageQueue(eventQueue);      // optional: use this if you want to process client events

            Client hosebirdClient = builder.build();

            // Attempts to establish a connection.
            hosebirdClient.connect();

            //create Producer
            KafkaProducer<String,String> producer = createKafkaProducer();
            String topic = "twitter_topic";
            String msg = "";
            //ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, msg);
            // Do whatever needs to be done with messages
            for (int msgRead = 0; msgRead < 1000; msgRead++) {
                if (hosebirdClient.isDone()) {
                    logger.error("Client connection closed unexpectedly: " + ((BasicClient) hosebirdClient).getExitEvent().getMessage());
                    break;
                }
                msg = msgQueue.poll(5, TimeUnit.SECONDS);

                //String msg = msgQueue.poll(5, TimeUnit.SECONDS);
                if (msg == null) {
                    System.out.println("Did not receive a message in 5 seconds");
                } else {
                    ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, msg);
                    //send data - async
                    producer.send(record, new Callback() {

                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            //executes for every record send
                            if (e == null) {
                                logger.info("Received new metadata. \n Topic:" + recordMetadata.topic() + "\n" +
                                        "Partition: " + recordMetadata.partition() + "\n" +
                                        "Offset: " + recordMetadata.offset() + "\n" +
                                        "Timestamp: " + recordMetadata.timestamp());
                            } else {
                                logger.error("Error while producing: ", e);
                            }
                        }
                    });
                }
            }

            hosebirdClient.stop();

            // Print some stats
            System.out.printf("The client read %d messages!\n", hosebirdClient.getStatsTracker().getNumMessages());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static KafkaProducer<String,String> createKafkaProducer(){
        String bootstrapServers = "127.0.0.1:9092";
        //create Producer props
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create Producer
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);
        return producer;
    }

    public static void main(String[] args) throws IOException {

        Config properties = new Config();
        Map<String, String> values = properties.getValues();
        String arg1 = values.get("tw_consumerKey").toString();
        System.out.println("arg1: " + arg1);
        TwitterConsumer.run(values.get("tw_consumerKey").toString(), values.get("tw_consumerSecret").toString(),
                values.get("tw_token").toString(), values.get("tw_secret").toString());
    }
}


