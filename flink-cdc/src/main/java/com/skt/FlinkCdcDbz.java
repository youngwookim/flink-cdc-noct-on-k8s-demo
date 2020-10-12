package com.skt;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkCdcDbz {

  public static void main(String[] args) throws Exception {
    final ParameterTool params = ParameterTool.fromArgs(args);

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    // DBZ Json
    tableEnv.executeSql(
        "CREATE TABLE test_customer_changelog_dbz (\n"
            + "  c_customer_sk BIGINT,\n"
            + "  c_customer_id STRING,\n"
            + "  c_current_cdemo_sk BIGINT,\n"
            + "  c_current_hdemo_sk BIGINT,\n"
            + "  c_current_addr_sk BIGINT,\n"
            + "  c_first_shipto_date_sk BIGINT,\n"
            + "  c_first_sales_date_sk BIGINT,\n"
            + "  c_salutation STRING,\n"
            + "  c_first_name STRING,\n"
            + "  c_last_name STRING,\n"
            + "  PRIMARY KEY (c_customer_sk) NOT ENFORCED "
            + ") WITH (\n"
            + " 'connector' = 'kafka',\n"
            + " 'topic' = 'test-customer-changelog-dbz',\n"
            + " 'properties.bootstrap.servers' = 'my-cluster-kafka-bootstrap:9092',\n"
            + " 'properties.group.id' = 'testGroup1234',\n"
            + " 'format' = 'debezium-json'\n"
            + ")");

    // Target table
    tableEnv.executeSql(
        "CREATE TABLE customer (\n"
            + "  c_customer_sk BIGINT,\n"
            + "  c_customer_id STRING,\n"
            + "  c_current_cdemo_sk BIGINT,\n"
            + "  c_current_hdemo_sk BIGINT,\n"
            + "  c_current_addr_sk BIGINT,\n"
            + "  c_first_shipto_date_sk BIGINT,\n"
            + "  c_first_sales_date_sk BIGINT,\n"
            + "  c_salutation STRING,\n"
            + "  c_first_name STRING,\n"
            + "  c_last_name STRING,\n"
            + "  PRIMARY KEY (c_customer_sk) NOT ENFORCED "
            + ") WITH (\n"
            + "   'connector' = 'jdbc',\n"
            + "   'url' = 'jdbc:mysql://mariadb.default:3306/test',\n"
            + "   'table-name' = 'customer',\n"
            + "   'username' = 'root',\n"
            + "   'password' = 'mypassword'\n"
            + ")");

    tableEnv.executeSql("INSERT INTO customer SELECT * FROM test_customer_changelog_dbz");
    //TableResult tr = tableEnv.executeSql("INSERT INTO customer SELECT * FROM test_customer_changelog_dbz");
    //System.out.println(tr.getJobClient().get().getJobStatus());
    
  }
}
