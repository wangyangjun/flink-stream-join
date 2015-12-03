package fi.aalto.dmg.data;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.UUID;

/**
 * Data generator for advertise click workload
 * Created by jun on 02/12/15.
 */
public class AdvertiseClickDataGenerator {
    private static final Logger logger = Logger.getLogger(AdvertiseClickDataGenerator.class);
    private static String ADV_TOPIC = "advertisement";
    private static String CLICK_TOPIC = "click";
    // The mean time of gap between two shown advertisements, 100 milliseconds
    private static double NEXT_ADV_LAMBDA = 50;
    // The mean time of gap between advertisement shown and clicked, 20 s
    private static double CLICK_LAMBDA = 20*1000;
    // delay of message arriving server
    private static double SIGMA = 10;

    private static double CLICK_PROBABILITY = 0.5;
    private static KafkaProducer<String, String> producer;

    private static KafkaProducer<String, String> createProducer(){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.RETRIES_CONFIG, "3");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 200);
        props.put(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG, true);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        return producer;
    }

    private static Connection conn = null;
    public static Connection getConnection(){
        if(null != conn) return conn;
        try {
            conn =
                    DriverManager.getConnection("jdbc:mysql://localhost/test?" +
                            "user=root&password=123456");
        } catch (SQLException ex) {
            // handle any errors
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        }
        return conn;
    }
    public static void closeConnect(){
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public static void insert(String tableName, String id, long time){
        Connection conn = getConnection();
        Statement stmt = null;

        try {
            stmt = conn.createStatement();
            stmt.execute("INSERT INTO " + tableName
                    + " VALUES('" + id + "'," + String.valueOf(time) + ")");
        }
        catch (SQLException ex){
            // handle any errors
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        }
    }

    public static void main( String[] args ) throws InterruptedException {
        RandomDataGenerator AdvertiseGenerator = new RandomDataGenerator();
        AdvertiseGenerator.reSeed(10000000L);



        if(null==producer){
            producer = createProducer();
        }
        // for loop to generate advertisement
        for (int i = 0; i < 10000; ++i) {
            String advId = UUID.randomUUID().toString();
            long currentTimeStamp = System.currentTimeMillis();

            System.out.println(String.valueOf(currentTimeStamp) + "\t" + advId);
            insert("advertisement", advId, currentTimeStamp);
            producer.send(new ProducerRecord<String, String>(ADV_TOPIC, String.format("%s %d", advId, currentTimeStamp)));

            // whether customer clicked this advertisement
            RandomDataGenerator clickGenerator = new RandomDataGenerator();
            if(clickGenerator.nextUniform(0,1)<=CLICK_PROBABILITY){
                // after delatT, user clicked the advertisement, milliseconds
                long deltaT = (long)clickGenerator.nextExponential(CLICK_LAMBDA);
                // message arrive delay, if delay > 0, then dealy of click is larger than shown
                // milliseconds
                long delay = (long)clickGenerator.nextGaussian(0, SIGMA)*1000;

                long clickTime = currentTimeStamp+deltaT+delay;
                System.out.println("Clicked: " + String.valueOf(clickTime) + "\t" + advId);
                insert("click", advId, clickTime);
                producer.send(new ProducerRecord<String, String>(CLICK_TOPIC, String.format("%s %d", advId, clickTime)));

            }

            long t = (long)AdvertiseGenerator.nextExponential(NEXT_ADV_LAMBDA);
            Thread.sleep(t);

        }
        closeConnect();
    }

}
