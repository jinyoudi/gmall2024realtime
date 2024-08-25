package com.atguigu.gmall.realtime.dim.test;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test01_FlinkCDC {
    public static void main(String[] args) throws Exception {
        //ToDO 1.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //todo 2.设置并行度
        env.setParallelism(1);
        //todo 3.source、使用flinkcdc读取mysql表中数据
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("gmall2024_config") // set captured database
                .tableList("gmall2024_config.t_user") // 库名.表名的形式
                .username("root")
                .password("000000")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .startupOptions(StartupOptions.initial()) //从什么位置开始读取，会读取历史数据
                .build();

        env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(),"MySQL Source")
                .print();

        //todo 4.执行
        env.execute("Print MySQL Snapshot + Binlog");
    }
}
