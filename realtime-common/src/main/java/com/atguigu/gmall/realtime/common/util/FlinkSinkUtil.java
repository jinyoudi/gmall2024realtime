package com.atguigu.gmall.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TableProcessDwd;
import com.atguigu.gmall.realtime.common.constant.Constant;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

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

    public static KafkaSink<Tuple2<JSONObject, TableProcessDwd>> getKafkaSink(){
        KafkaSink<Tuple2<JSONObject, TableProcessDwd>> kafkaSink = KafkaSink.<Tuple2<JSONObject, TableProcessDwd>>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS) //设置服务器地址
                .setRecordSerializer(new KafkaRecordSerializationSchema<Tuple2<JSONObject, TableProcessDwd>>() {
                    //自己实现序列化
                    @Nullable
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(Tuple2<JSONObject, TableProcessDwd> tup2, KafkaSinkContext kafkaSinkContext, Long aLong) {
                        JSONObject jsonObj = tup2.f0;
                        TableProcessDwd tableProcessDwd = tup2.f1;
                        String topic = tableProcessDwd.getSinkTable();
                        return new ProducerRecord<byte[], byte[]>(topic, jsonObj.toJSONString().getBytes());
                    }
                })
                //当前配置决定是否开启事务，保证写入kafka数据的精准一次
//                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                //设置事务id的前缀
//                .setTransactionalIdPrefix("dwd_base_log_")
                //设置事务的超时时间
//                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,15*60*1000+"") //检查点超时时间<事务的超时时间<=事务最大超时时间
                .build();
        return kafkaSink;
    }

    //扩展，如果流中数据类型不确定，如何将数据写到kafka主题
    public static <T>KafkaSink<T> getKafkaSink(KafkaRecordSerializationSchema<T> ksr){
        KafkaSink<T> kafkaSink = KafkaSink.<T>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS) //设置服务器地址
                .setRecordSerializer(ksr)
                //当前配置决定是否开启事务，保证写入kafka数据的精准一次
//                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                //设置事务id的前缀
//                .setTransactionalIdPrefix("dwd_base_log_")
                //设置事务的超时时间
//                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,15*60*1000+"") //检查点超时时间<事务的超时时间<=事务最大超时时间
                .build();
        return kafkaSink;
    }

    //获取DorisSink
    public static DorisSink<String> getDorisSink(String tableName){
        Properties props = new Properties();
        props.setProperty("format", "json");
        props.setProperty("read_json_by_line", "true"); // 每行一条 json 数据

        DorisSink<String> sink = DorisSink.<String>builder()
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisOptions(DorisOptions.builder() // 设置 doris 的连接参数
                        .setFenodes("hadoop102:7030")
                        .setTableIdentifier(Constant.DORIS_DATABASE + "." + tableName)
                        .setUsername("root")
                        .setPassword("aaaaaa")
                        .build())
                .setDorisExecutionOptions(DorisExecutionOptions.builder() // 执行参数
                        //.setLabelPrefix("doris-label")  // stream-load 导入的时候的 label 前缀
                        .disable2PC() // 开启两阶段提交后,labelPrefix 需要全局唯一,为了测试方便禁用两阶段提交
                        .setDeletable(false)
                        .setBufferCount(3) // 批次条数: 默认 3
                        .setBufferSize(8*1024) // 批次大小: 默认 1M
                        .setCheckInterval(3000) // 批次输出间隔   三个对批次的限制是或的关系
                        .setMaxRetries(3)
                        .setStreamLoadProp(props) // 设置 stream load 的数据格式 默认是 csv,根据需要改成 json
                        .build())
                .setSerializer(new SimpleStringSerializer())
                .build();
        return sink;
    }


}
