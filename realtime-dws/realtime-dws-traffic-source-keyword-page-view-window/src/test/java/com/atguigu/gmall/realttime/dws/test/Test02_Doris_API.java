package com.atguigu.gmall.realttime.dws.test;

/*
演示flink读取Doris API
 */

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.deserialization.SimpleListDeserializationSchema;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.doris.flink.source.DorisSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.Properties;

public class Test02_Doris_API {
    public static void main(String[] args) throws Exception {
        //todo 1.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //todo 2.检查点相关配置
        env.enableCheckpointing(5000L);
        //todo 3.从Doris表中读取数据
//        DorisOptions.Builder builder = DorisOptions.builder()
//                .setFenodes("hadoop102:7030")
//                .setTableIdentifier("test.table1")
//                .setUsername("root")
//                .setPassword("aaaaaa");
//
//        DorisSource<List<?>> dorisSource = DorisSource.<List<?>>builder()
//                .setDorisOptions(builder.build())
//                .setDorisReadOptions(DorisReadOptions.builder().build())
//                .setDeserializer(new SimpleListDeserializationSchema())
//                .build();
//
//        DataStreamSource<List<?>> stream1 = env.fromSource(dorisSource, WatermarkStrategy.noWatermarks(), "doris source");
//        stream1.print();

        //todo 4.向Doris表中写入数据
        DataStreamSource<String> source = env
                .fromElements(
                        "{\"siteid\": \"550\", \"citycode\": \"1001\", \"username\": \"ww\",\"pv\": \"100\"}");
        Properties props = new Properties();
        props.setProperty("format", "json");
        props.setProperty("read_json_by_line", "true"); // 每行一条 json 数据

        DorisSink<String> sink = DorisSink.<String>builder()
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisOptions(DorisOptions.builder() // 设置 doris 的连接参数
                        .setFenodes("hadoop102:7030")
                        .setTableIdentifier("test.table1")
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
        source.sinkTo(sink);


        env.execute();

    }
}
