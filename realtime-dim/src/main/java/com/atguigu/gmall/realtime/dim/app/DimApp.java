package com.atguigu.gmall.realtime.dim.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.FlinkSourceUtil;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.atguigu.gmall.realtime.dim.function.HBaseSinkFunction;
import com.atguigu.gmall.realtime.dim.function.TableProcessFunction;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
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
 *开发流程
 *  基本环境准备
 *  检查点相关设置
 *  从kafka主题中读取数据
 *  对流中数据进行类型转换并etl     jsonStr->jsonObj
 * ------------------------------
 *  使用flinkcdc读取配置表中的配置信息
 *  对读取配置流数据进行类型转换 jsonStr->实体类对象
 *  根据当前配置信息到HBase中执行建表或者删除操作
 *      op=d  删表
 *      op=c、r  建表
 *      op=u  先删表、再建表
 *  对配置流数据进行广播--broadcast
 *  关联主流业务数据以及广播流配置数据--connect
 *  对关联后的数据进行处理--process
 *      new TableProcessFunction extends BroadcastProcessFunction{
 *          open;将配置信息预加载到程序中，避免主流数据先到，广播流数据后到，丢失数据的情况
 *          processElement;对主流数据的处理
 *              获取操作的表的表名
 *              根据表名到广播状态中以及configMap中获取对应的配置信息，如果配置信息不为空，说明是维度，将维度数据发送到下游
 *                  Tuple2<dataJsonObj,配置对象>
 *              在向下游发送数据之前，过滤掉了不需要传递的属性，补充操作类型
 *          processBroadcastElement;对广播流数据进行出处理
 *              op=d 将配置信息从广播状态以及configmap中删除掉
 *              op!=d 将配置信息放到广播状态以及configmap中
 *      }
 *  将流中的数据同步到HBase中
 *      class HBaseSinkFunction extends RichSinkFunction{
 *          invoke;
 *              type="delete" 从HBase中删除数据
 *              type!="delete" 从HBase表中put数据
 *      }
 *  优化：抽取FlinkSourceUtil 工具类
 *      抽取TableProcessFunction以及HBaseSinkFuction函数处理
 *      抽取方法
 *      抽取基类---模板方法设计模式
 *
 * 执行流程（以修改了品牌维度表中的一条数据为例）
 *      当程序开始启动的时候，会将配置表中的配置信息加载到configMap以及广播状态中
 *      修改品牌为度
 *      binlog会将修改操作记录下来
 *      maxwell会从binlog中获取修改的信息，并封装为json格式字符串，发送到kafka到topic_db主题日中
 *      DimApp应用程序会从topic_db主题中读取数据并对其进行处理
 *      根据当前数据的表名判断是否为维度
 *      如果是维度数据，将维度数据传递到下游
 *      将维度数据同步到HBase中
 */

public class DimApp extends BaseApp {
    public static void main(String[] args) throws Exception {
        /**
         * （1）DIM层维度分流应用使用10001端口
         * （2）DWD层应用程序按照在本文档中出现的先后顺序，端口从10011开始，自增1
         * （3）DWS层应用程序按照在本文档中出现的先后顺序，端口从10021开始，自增1
         */
        //如果报了端口号冲突，那就换个
        //could not start rest endpoint on any port in port range xxx
        new DimApp().start(10001,4,"dim_app",Constant.TOPIC_DB);
    }

    @Override
    public void hadle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDs) {
        //这里的etl快捷实现方式是，选中那一串代码的方法 ctrl+alt+m
        //todo 对业务流中数据类型进行转换并且做个简单的etl  jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = etl(kafkaStrDs);

        //todo 使用FlinkCDC读取配置表中的配置信息
        SingleOutputStreamOperator<TableProcessDim> tpDS = readTableProcess(env);

        //todo 根据配置表中的配置信息到HBase中执行建表或者删除表操作
        tpDS = createHbaseTable(tpDS);
        //执行的时候记得先在hbase里创建表空间：create_namespace "gmall2024"
        //hbase shell 进入客户端。  list_namespace 查看空间。  list_namespace_tables "gmall2024" 查看这个空间下有多少表
        //desc "gmall2024:dim_base_trademark" 查看xx空间下的xx表
//        tpDS.print();   //测试数据
        //todo 过滤维度数据
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS = connect(jsonObjDS, tpDS);

        //todo 将维度数据同步到HBase表中
        //数据格式 ({"tm_name":"Redmi","id":1,"type":"update"},TableProcessDim(sourceTable=base_trademark, sinkTable=dim_base_trademark, sinkColumns=id,tm_name, sinkFamily=info, sinkRowKey=id, op=r))
        dimDS.print();
        writeToHBase(dimDS);
    }

    private static void writeToHBase(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS) {
        dimDS.addSink(new HBaseSinkFunction());
    }

    private static SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> connect(SingleOutputStreamOperator<JSONObject> jsonObjDS, SingleOutputStreamOperator<TableProcessDim> tpDS) {
        //todo 8.将配置流中的配置信息进行过广播--broadcast
        //广播流是个kv形式的。key是表名，value是TableProcessDim的对象
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor = new MapStateDescriptor<String, TableProcessDim>("mapStateDescriptor",String.class,TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastDS = tpDS.broadcast(mapStateDescriptor);

        //todo 9.将主流业务数据和广播配置流配置信息进行关联---connect
        //jsonObjDS是主流，broadcastDS是广播流
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectDS = jsonObjDS.connect(broadcastDS);

        //todo 10.处理关联后的数据（判断是否为维度）
        SingleOutputStreamOperator<Tuple2<JSONObject,TableProcessDim>> dimDS = connectDS.process(
                new TableProcessFunction(mapStateDescriptor)
        );
        return dimDS;
    }

    private static SingleOutputStreamOperator<TableProcessDim> createHbaseTable(SingleOutputStreamOperator<TableProcessDim> tpDS) {
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
        return tpDS;
    }

    private static SingleOutputStreamOperator<TableProcessDim> readTableProcess(StreamExecutionEnvironment env) {
        //5.1 创建MySqlSource对象
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource("gmall2024_config", "table_process_dim");

        //5.2 读取数据 封装为流
        DataStreamSource<String> mysqlStrDS = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source")
                .setParallelism(1);
        ;
//        mysqlStrDS.print();

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
        //tpDS.print();
        return tpDS;
    }

    private static SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> kafkaStrDs) {

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
        return jsonObjDS;
    }
}
