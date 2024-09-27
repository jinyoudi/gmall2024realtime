package com.atguigu.gmall.realtime.common.util;

import com.atguigu.gmall.realtime.common.constant.Constant;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;

public class FlinkSinkUtil {
    //获取kafkasink
    public static KafkaSink<String> getKafkaSink(String topic){
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS) //设置服务器地址
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                //当前配置决定是否开启事务，保证写入kafka数据的精准一次
//                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                //设置事务id的前缀
//                .setTransactionalIdPrefix("dwd_base_log_")
                //设置事务的超时时间
//                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,15*60*1000+"") //检查点超时时间<事务的超时时间<=事务最大超时时间
                .build();
        return kafkaSink;
    }
}
