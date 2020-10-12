package com.skt;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import io.debezium.data.Envelope;

public class FlinkCdcNoct {

  public static void main(String[] args) throws Exception {
    final ParameterTool parameterTool = ParameterTool.fromArgs(args);

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    Properties props = new Properties();
    props.setProperty("bootstrap.servers", "my-cluster-kafka-bootstrap:9092");
    props.setProperty("group.id", "testGroup");

    DataStream<String> stream =
        env.addSource(
            new FlinkKafkaConsumer<>(
                "test-customer-changelog-raw", new SimpleStringSchema(), props));

    DataStream<String> dbz =
        stream.map(
            new MapFunction<String, String>() {

              @Override
              public String map(String value) throws Exception {
                // FIXME
                String[] noct = value.split(",");

                Schema recordSchema =
                    SchemaBuilder.struct()
                        .name("com.skt.customer")
                        .doc("customer")
                        .field("c_customer_sk", Schema.INT32_SCHEMA)
                        .field("c_customer_id", Schema.STRING_SCHEMA)
                        .field("c_current_cdemo_sk", Schema.INT32_SCHEMA)
                        .field("c_current_hdemo_sk", Schema.INT32_SCHEMA)
                        .field("c_current_addr_sk", Schema.INT32_SCHEMA)
                        .field("c_first_shipto_date_sk", Schema.INT32_SCHEMA)
                        .field("c_first_sales_date_sk", Schema.INT32_SCHEMA)
                        .field("c_salutation", Schema.STRING_SCHEMA)
                        .field("c_first_name", Schema.STRING_SCHEMA)
                        .field("c_last_name", Schema.STRING_SCHEMA)
                        .build();

                Schema sourceSchema =
                    SchemaBuilder.struct()
                        .name("com.example.customer.source")
                        .doc("")
                        .field("table", Schema.STRING_SCHEMA)
                        .build();

                Envelope e =
                    Envelope.defineSchema()
                        .withName("test")
                        .withDoc("test doc")
                        .withRecord(recordSchema)
                        .withSource(sourceSchema)
                        .build();

                System.out.println(e.schema());
                
                Struct record =
                    new Struct(recordSchema)
                        .put("c_customer_sk", Integer.parseInt(noct[1]))
                        .put("c_customer_id", noct[2])
                        .put("c_current_cdemo_sk", (noct[3].equals(""))? null : Integer.parseInt(noct[3]))
                        .put("c_current_hdemo_sk", (noct[4].equals(""))? null : Integer.parseInt(noct[4]))
                        .put("c_current_addr_sk", (noct[5].equals(""))? null : Integer.parseInt(noct[5]))
                        .put("c_first_shipto_date_sk", (noct[6].equals(""))? null : Integer.parseInt(noct[6]))
                        .put("c_first_sales_date_sk", (noct[7].equals(""))? null : Integer.parseInt(noct[7]))
                        .put("c_salutation", noct[8])
                        .put("c_first_name", noct[9])
                        .put("c_last_name", noct[10]);

                Struct ss = new Struct(sourceSchema).put("table", "customer");

                Instant ts = Instant.now();

                Struct s = null;
                if (noct[0].startsWith("IA")) {
                  s = e.create(record, ss, ts);
                }
                if (noct[0].startsWith("UA")) {
                  // before?
                  s = e.update(record, record, ss, ts);
                }
                if (noct[0].startsWith("DB")) {
                  s = e.delete(record, ss, ts);
                }

                Map configs = new HashMap();
                configs.put("schemas.enable", false);
                configs.put("converter.type", "value");

                JsonConverter jc = new JsonConverter();
                jc.configure(configs);

                byte[] o = jc.fromConnectData("", e.schema(), s);
                String json = new String(o, "UTF-8");
                System.out.println(json);

                return json;
              }
            });

    FlinkKafkaProducer<String> output =
        new FlinkKafkaProducer<>("test-customer-changelog-dbz", new SimpleStringSchema(), props);

    dbz.addSink(output);

    env.execute("Flink Streaming ...");
  }
}
