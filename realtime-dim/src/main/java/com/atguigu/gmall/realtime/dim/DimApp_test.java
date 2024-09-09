package com.atguigu.gmall.realtime.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.google.gson.JsonObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import javafx.scene.control.Tab;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.*;

/**
 * 需要启动的进程、可以按照下面顺序开启
 *  zk、kafka、maxwell、hdfs、HBase、DimApp_test
 */

public class DimApp_test {
    public static void main(String[] args) throws Exception {
        //todo 1.基本环境准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
        //todo 2.检查点相关的设置
        //2.1 开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE); //5秒，精准一次性
        //2.2 设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000L); //一分钟
        //2.3 设置job取消后检查点是否保留
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4 设置两个检查点之间最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L); //如果说默认只能有一个检查点备份（可以设置多个），第1个检查点6s才备份完，那第二个检查点按理说5s开启，但现在只能8s的时候才能开启，因为最小间隔2s
        //2.5 重启策略，默认的是Integer的最大值
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L)); //重启3次，3s尝试一次
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3))); //30天只能重启三次
        //2.6 设置状态后端以及检查点存储路径
//        env.setStateBackend(new HashMapStateBackend()); //状态存在tm的堆内存里，检查点存在Jm的堆内存里
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/ck"); //有可能是hadoop102
        //2.7 设置操作hadoop的用户
        System.setProperty("HADOOP_USER_NAME","atguigu");

        //todo 3.从kafka的topic_db主题中读取业务数据
        //3.1 声明消费的主题以及消费者组
        String topic = "topic_db";
        String groupId = "dim_app_group";
        //3.2 创建消费者对象
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
                .setTopics(topic)
                .setGroupId(groupId)
                //在生产环境中，一般为了保证消费的精准一次性，需要手动维护偏移量，KafkaSource->KafkaSourceReader->存储偏移量变量
//                    // 先从手动维护偏移量开始消费，如果没找到，从最新位点开始消费
//                    .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setStartingOffsets(OffsetsInitializer.latest())  //从最末尾点开始消费
//                .setValueOnlyDeserializer(new SimpleStringSchema()) //如果使用flink提供的SimpleStringSchema对string类型的消息反序列化，如果消息为空，会报错
                .setValueOnlyDeserializer(
                        new DeserializationSchema<String>() {
                            @Override
                            public String deserialize(byte[] bytes) throws IOException {
                                if(bytes !=null){
                                    return new String(bytes);
                                }
                                return null;
                            }

                            @Override
                            public boolean isEndOfStream(String s) {
                                return false;
                            }

                            @Override
                            public TypeInformation<String> getProducedType() {
                                return TypeInformation.of(String.class);
                            }
                        }
                )
                .build();

        //3.3 消费数据 封装为流
        DataStreamSource<String> kafkaStrDs = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka_Source");

        //todo 4.对业务流中数据类型进行转换并且做个简单的etl  jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDs.process(
                new ProcessFunction<String, JSONObject>() { //传进来String，传出去JSONObject
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        //字符串格式大概的样子
                        /*
                        {
                            "database":"gmall2024",
                            "table":"base_trademark",
                            "type":"update",
                            "ts":1710075970,
                            "data":{"id":1,"tm_name":"Redmi","logo_url":"abc","create_time":"2021-12014 00:00:00","operate_time":null},
                            "old":{"tm_name":"Redmi111"}
                        }
                         */
                        JSONObject jsonObject = JSON.parseObject(jsonStr); //把json字符串转换成Object
                        String db = jsonObject.getString("database"); //获取数据库名称
                        String type = jsonObject.getString("type"); //获取操作类型
                        String data = jsonObject.getString("data"); //获取变动data

                        if ("gmall2024".equals(db)
                                && ("insert".equals(type)
                                || "update".equals(type)
                                || "delete".equals(type)
                                || "bootstrap-insert".equals(type)) //维度历史数据的处理，会把历史数据同步过来
                                && data != null
                                && data.length() > 2
                        ) {
                            out.collect(jsonObject);
                        }
                    }
                }
        );

//        jsonObjDS.print(); //验证数据能否从业务库到kafka并且输出到控制台

        //todo 5.使用FlinkCDC读取配置表中的配置信息
        //5.1 创建MySqlSource对象
        Properties props = new Properties();
        //使用用户名密码登录
        props.setProperty("useSSL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("gmall2024_config")
                .tableList("gmall2024_config.table_process_dim")
                .username("root")
                .password("000000")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .jdbcProperties(props) //jdbc的设置
                .build();
        //5.2 读取数据 封装为流
        DataStreamSource<String> mysqlStrDS = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source")
                .setParallelism(1);
        //数据格式
        //1——op=r> {"before":null,"after":{"source_table":"coupon_info","sink_table":"dim_coupon_info","sink_family":"info","sink_columns":"id,coupon_name,coupon_type,condition_amount,condition_num,activity_id,benefit_amount,benefit_discount,create_time,range_type,limit_num,taken_count,start_time,end_time,operate_time,expire_time,range_desc","sink_row_key":"id"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":0,"snapshot":"false","db":"gmall2024_config","sequence":null,"table":"table_process_dim","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1724687527724,"transaction":null}
        //2——op=c> {"before":null,"after":{"source_table":"a","sink_table":"a","sink_family":"a","sink_columns":"aaa","sink_row_key":"a"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1724687425000,"snapshot":"false","db":"gmall2024_config","sequence":null,"table":"table_process_dim","server_id":1,"gtid":null,"file":"mysql-bin.000003","pos":11426524,"row":0,"thread":71,"query":null},"op":"c","ts_ms":1724687425992,"transaction":null}
        //3——op=u> {"before":{"source_table":"a","sink_table":"a","sink_family":"a","sink_columns":"aaa","sink_row_key":"a"},"after":{"source_table":"a","sink_table":"a","sink_family":"a","sink_columns":"aaabbb","sink_row_key":"a"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1724687584000,"snapshot":"false","db":"gmall2024_config","sequence":null,"table":"table_process_dim","server_id":1,"gtid":null,"file":"mysql-bin.000003","pos":11426872,"row":0,"thread":71,"query":null},"op":"u","ts_ms":1724687584782,"transaction":null}
        //4——op=d> {"before":{"source_table":"a","sink_table":"a","sink_family":"a","sink_columns":"aaabbb","sink_row_key":"a"},"after":null,"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1724687619000,"snapshot":"false","db":"gmall2024_config","sequence":null,"table":"table_process_dim","server_id":1,"gtid":null,"file":"mysql-bin.000003","pos":11427233,"row":0,"thread":71,"query":null},"op":"d","ts_ms":1724687619407,"transaction":null}

        //mysqlStrDS.print(); //测试mysql配置表信息是否可以同步

        //todo 6.对配置流中的数据类型进行转换 jsonStr->实体类对象
        SingleOutputStreamOperator<TableProcessDim> tpDS = mysqlStrDS.map(
                new MapFunction<String, TableProcessDim>() {
                    @Override
                    public TableProcessDim map(String jsonStr) throws Exception {
                        //为了处理方便，先将jsonStr转换为jsonObj。
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        String op = jsonObj.getString("op");
                        TableProcessDim tableProcessDim = null;
                        if("d".equals(op)){
                            //对配置表进行了一次删除操作 从before属性中获取删除钱的配置信息
                            tableProcessDim = jsonObj.getObject("before", TableProcessDim.class); //数据库里字段是蛇形名命法，TableProcessDim里是驼峰命名法，阿里巴巴fastjson底层可以自动把蛇形转换成驼峰，然后给类中的属性赋值
                        }else{
                            //对配置表进行了读取，添加，修改操作  从after属性中获取最新的配置信息
                            tableProcessDim = jsonObj.getObject("after",TableProcessDim.class);
                        }
                        tableProcessDim.setOp(op);
                        return tableProcessDim;
                    }
                }
        ).setParallelism(1);
//        tpDS.print();

        //todo 7.根据配置表中的配置信息到HBase中执行建表或者删除表操作
        tpDS = tpDS.map(
                new RichMapFunction<TableProcessDim, TableProcessDim>() {

                    private Connection hbaseConn;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = HBaseUtil.getHbaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHbaseConnection(hbaseConn);
                    }

                    @Override
                    public TableProcessDim map(TableProcessDim tp) throws Exception {
                        //获取对配置表进行的操作的类型
                        String op = tp.getOp();
                        //获取hbase中维度表的表名
                        String sinkTable = tp.getSinkTable();
                        //获取列族
                        String[] sinkFamilies = tp.getSinkFamily().split(",");
                        if("d".equals(op)){
                            //从配置表中删除了一条数据 将HBase中对应的表删除掉
                            HBaseUtil.dropHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE,sinkTable); //连接是一个重操作，在map里不合适,因此用富函数，写在open里
                        }else if("r".equals(op) || "c".equals(op)){
                            //从配置表中读取了一条数据或向配置表中添加了一条配置 在hbase中执行建表
                            HBaseUtil.createHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable,sinkFamilies);
                        }else{
                            //对配置表信息进行了修改，先从Hbase中将对应的表删除掉，再创建新表
                            HBaseUtil.dropHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE,sinkTable);
                            HBaseUtil.createHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable,sinkFamilies);
                        }
                        return tp;
                    }
                }
        ).setParallelism(1);
        //执行的时候记得先在hbase里创建表空间：create_namespace "gmall2024"
        //hbase shell 进入客户端。  list_namespace 查看空间。  list_namespace_tables "gmall2024" 查看这个空间下有多少表
        //desc "gmall2024:dim_base_trademark" 查看xx空间下的xx表
//        tpDS.print();   //测试数据

        //todo 8.将配置流中的配置信息进行过广播--broadcast
        //广播流是个kv形式的。key是表名，value是TableProcessDim的对象
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor = new MapStateDescriptor<String, TableProcessDim>("mapStateDescriptor",String.class,TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastDS = tpDS.broadcast(mapStateDescriptor);

        //todo 9.将主流业务数据和广播配置流配置信息进行关联---connect
        //jsonObjDS是主流，broadcastDS是广播流
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectDS = jsonObjDS.connect(broadcastDS);

        //todo 10.处理关联后的数据（判断是否为维度）
        SingleOutputStreamOperator<Tuple2<JSONObject,TableProcessDim>> dimDS = connectDS.process(
                //第一个参数：第一条流的类型。第二个参数，第二条流的类型。第三个参数，输出的类型是什么(二元组，第一个是影响的那行数据，第二个是这条维度数据的配置信息)
                new BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject,TableProcessDim>>() {

                    //第一个参数表名 第二个参数配置流
                    //不要怀疑是否有了configmap之后，广播状态是否就没用了。他俩作用范围是一样，对当前算子并行度有效，但是广播状态会被检查点做备份，而configmap不会，因此任务如果不是重启而是从检查点恢复，那configmap的open就不会再执行了。open只在重启的时候执行
                    private Map<String,TableProcessDim> configMap = new HashMap<>();
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //将配置表中的配置信息预加载到configMap中
                        //注册驱动
                        Class.forName("com.mysql.cj.jdbc.Driver");
                        //建立连接
                        java.sql.Connection conn = DriverManager.getConnection(Constant.MYSQL_URL, Constant.MYSQL_USER_NAME, Constant.MYSQL_PASSWORD);
                        //获取数据库操作对象
                        String sql = "select * from gmall2024_config.table_process_dim";
                        PreparedStatement ps = conn.prepareStatement(sql);
                        //执行sql语句
                        ResultSet rs = ps.executeQuery();
                        ResultSetMetaData metaData = rs.getMetaData(); //获取元数据信息
                        //处理结果集
                        while(rs.next()){
                            JSONObject jsonObj = new JSONObject();
                            //jdbc是个例外，从1开始
                            for (int i = 1; i <= metaData.getColumnCount() ; i++) {
                                String columnName = metaData.getColumnName(i);
                                Object columnValue = rs.getObject(i);
                                jsonObj.put(columnName,columnValue);
                            }
                            //将jsonObj转换为实体类对象，并放到configMap中
                            TableProcessDim tableProcessDim = jsonObj.toJavaObject(TableProcessDim.class);
                            configMap.put(tableProcessDim.getSourceTable(),tableProcessDim);
                        }

                        //释放资源
                        rs.close();
                        ps.close();
                        conn.close();
                    }

                    //processElement处理主流业务数据 根据维度表名到广播状态中读取配置信息，判断是否为维度
                    @Override
                    public void processElement(JSONObject jsonObj, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject,TableProcessDim>>.ReadOnlyContext ctx, Collector<Tuple2<JSONObject,TableProcessDim>> collector) throws Exception {
                        //获取处理的数据的表名
                        String table = jsonObj.getString("table");
                        //获取广播状态
                        ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                        //根据表名，先到广播状态中获取对应的配置信息，如果没有找到对应的配置，再尝试到configmap中获取
                        TableProcessDim tableProcessDim = null;
                        //先走的广播状态，后走的configmap
                        if((tableProcessDim = broadcastState.get(table)) != null
                        || (tableProcessDim = configMap.get(table)) != null ){
                            //如果根据表名获取到了对应的配置信息，说明当前处理的是维度数据，将维度数据继续向下游传递

                            //将维度数据继续向下游传递（只需传递data属性内容即可）
                            JSONObject dataJsonObj = jsonObj.getJSONObject("data");

                            //在向下游传递数据之前，过滤掉不需要传递的属性
                            String sinkColumns = tableProcessDim.getSinkColumns();
                            //定义了一个过滤方法
                            deleteNotNeedColumns(dataJsonObj,sinkColumns);

                            //在向下游传递数据钱，补充对应维度数据的操作类型属性
                            String type = jsonObj.getString("type");
                            dataJsonObj.put("type",type);

                            collector.collect(Tuple2.of(dataJsonObj,tableProcessDim));
                        }
                    }

                    //processBroadcastElement处理广播流配置信息 将配置数据放到广播状态中或从广播状态中删除对应的配置 k：维度表名 v：一个配置对象
                    @Override
                    public void processBroadcastElement(TableProcessDim tp, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject,TableProcessDim>>.Context ctx, Collector<Tuple2<JSONObject,TableProcessDim>> collector) throws Exception {
                        //获取对配置表进行的操作的类型
                        String op = tp.getOp();
                        //获取广播状态
                        BroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                        //获取维度表名称
                        String sourceTable = tp.getSourceTable();
                        if("d".equals(op)){
                            //从配置表中删除了一条数据，将对应的配置信息也从广播状态中删除
                            broadcastState.remove(sourceTable);
                            //configMap里面也得删掉，
                            configMap.remove(sourceTable);
                        }else{
                            //可能对配置表进行读取 添加 或者更新操作，需要将最新配置信息放到广播状态中
                            broadcastState.put(sourceTable,tp);
                            configMap.put(sourceTable,tp); //这里可以不加，加不加无所谓
                        }

                    }
                }
        );

        //todo 11.将维度数据同步到HBase表中
        //数据格式 ({"tm_name":"Redmi","id":1,"type":"update"},TableProcessDim(sourceTable=base_trademark, sinkTable=dim_base_trademark, sinkColumns=id,tm_name, sinkFamily=info, sinkRowKey=id, op=r))
        dimDS.print();

        dimDS.addSink(new RichSinkFunction<Tuple2<JSONObject, TableProcessDim>>() {
            private Connection hbaseConn;
            @Override
            public void open(Configuration parameters) throws Exception {
                hbaseConn = HBaseUtil.getHbaseConnection();
            }

            @Override
            public void close() throws Exception {
                HBaseUtil.closeHbaseConnection(hbaseConn);
            }
            //将流中数据写出到HBase中
            @Override
            public void invoke(Tuple2<JSONObject, TableProcessDim> tup, Context context) throws Exception {
                JSONObject jsonObj = tup.f0;
                TableProcessDim tableProcessDim = tup.f1;
                String type = jsonObj.getString("type");
                jsonObj.remove("type");

                //获取操作的HBase表的表名
                String sinkTable = tableProcessDim.getSinkTable();
                //获取rowkey、要根据rowkey的值来删数据，所以tableProcessDim.getSinkRowKey()是不对的
                String rowKey = jsonObj.getString(tableProcessDim.getSinkRowKey());
                //判断对业务数据库维度表进行了什么操作
                if("delete".equals(type)){
                    //从业务数据库中维度表中做了删除操作 需要将HBase维度表中对应的记录也删除掉
                    HBaseUtil.delRow(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable,rowKey);
                }else{
                    //如果不是delete，可能的类型有 insert update bootstrap-insert 上述操作对应的都是向HBase表中put数据
                    String sinkFamily = tableProcessDim.getSinkFamily();
                    HBaseUtil.putRow(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable,rowKey,sinkFamily,jsonObj);
                }
            }
        });

        env.execute();
    }

    //过滤掉不需要传递的字段
    //dataJsonObj {"tm_name":"Redmi","create_time":"2021-12-14 00:00:00","logo_url":"122","id":1}
    //sinkColumns id,tm_name
    private static void deleteNotNeedColumns(JSONObject dataJsonObj, String sinkColumns) {
        List<String> columnList = Arrays.asList(sinkColumns.split(","));

        Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();

        //如果sinkColumns里面不包含dataJsonObj里面的key，那dataJsonObj删除这个key
        entrySet.removeIf(entry-> !columnList.contains(entry.getKey()));
    }
}
