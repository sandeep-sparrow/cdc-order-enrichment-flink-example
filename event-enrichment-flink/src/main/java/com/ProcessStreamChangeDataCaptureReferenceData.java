package com;

import com.operators.ChangeDataCaptureEnrichmentData;
import com.pojo.Order;
import com.pojo.Rate;
import com.utils.KafkaInitialiser;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessStreamChangeDataCaptureReferenceData {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessStreamChangeDataCaptureReferenceData.class);
    private static final String DEFAULT_SOURCE_TOPIC = "orders";
    private static final String DEFAULT_REFERENCE_DATA_TOPIC = "rates";

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, 8081);
        conf.set(RestOptions.BIND_PORT, "8081");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.enableCheckpointing(100000); // Enable checkpointing every 1 min

        //read the parameters specified from the command line
        ParameterTool parameter = ParameterTool.fromArgs(args);

        KafkaInitialiser ki = new KafkaInitialiser();

        //Create Kafka source - Rate
        DataStream<Rate> rateStream = ki.createRateStream(env, parameter, DEFAULT_REFERENCE_DATA_TOPIC);

        //Create Kafka source - Order
        DataStream<Order> orderStream = ki.createOrderStream(env, parameter, DEFAULT_SOURCE_TOPIC);

        //Connect Order and Rate stream and apply process function
        DataStream<Order> enrichedOrderStream = orderStream
                .connect(rateStream)
                .keyBy(order -> order.getCurrency(), rate -> rate.getCurrency())
                .process(new ChangeDataCaptureEnrichmentData());

        //Print the enriched stream in stdout
        enrichedOrderStream.print();

        LOG.info("Reading events from topic {} & {}", DEFAULT_SOURCE_TOPIC, DEFAULT_REFERENCE_DATA_TOPIC);

        env.execute();
    }
}
