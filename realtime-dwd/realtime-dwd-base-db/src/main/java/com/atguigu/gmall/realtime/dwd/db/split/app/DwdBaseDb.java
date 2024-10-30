package com.atguigu.gmall.realtime.dwd.db.split.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TableProcessDwd;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSourceUtil;
import com.atguigu.gmall.realtime.dwd.db.split.function.BaseDbTableProcessFunction;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/*
处理逻辑比较简单的事实表，动态分流处理
需要启动的进程
    zk kafka maxwell DwdBaseDb
 */
public class DwdBaseDb extends BaseApp {

    public static void main(String[] args) throws Exception {
        new DwdBaseDb().start(10019,
                4,
                "dwd_base_db",
                Constant.TOPIC_DB
        );
    }

    @Override
    public void hadle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDs) {
        //todo 对流中的数据进行类型转换并进行简单的etl jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDs.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    String type = jsonObj.getString("type");
                    if (!type.startsWith("bootstrap-")) { //如果不是维度历史数据，往下游传递
                        out.collect(jsonObj);
                    }
                } catch (Exception e) {
                    throw new RuntimeException("不是一个标准的json");
                }
            }
        });
//        jsonObjDS.print();
        //todo 使用flinkcdc读取配置表中的配置信息
        //创建mysqlsource对象
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource("gmall2024_config", "table_process_dwd");
        //读取数据 封装为流
        DataStreamSource<String> mysqlStrDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source");
        //对流中的数据进行类型转换 jsonStr->实体类对象
        SingleOutputStreamOperator<TableProcessDwd> tpDS = mysqlStrDS.map(
                new MapFunction<String, TableProcessDwd>() {
            @Override
            public TableProcessDwd map(String jsonStr) throws Exception {
                //数据格式
                //1——op=r> {"before":null,"after":{"source_table":"coupon_info","sink_table":"dim_coupon_info","sink_family":"info","sink_columns":"id,coupon_name,coupon_type,condition_amount,condition_num,activity_id,benefit_amount,benefit_discount,create_time,range_type,limit_num,taken_count,start_time,end_time,operate_time,expire_time,range_desc","sink_row_key":"id"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":0,"snapshot":"false","db":"gmall2024_config","sequence":null,"table":"table_process_dim","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1724687527724,"transaction":null}
                //2——op=c> {"before":null,"after":{"source_table":"a","sink_table":"a","sink_family":"a","sink_columns":"aaa","sink_row_key":"a"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1724687425000,"snapshot":"false","db":"gmall2024_config","sequence":null,"table":"table_process_dim","server_id":1,"gtid":null,"file":"mysql-bin.000003","pos":11426524,"row":0,"thread":71,"query":null},"op":"c","ts_ms":1724687425992,"transaction":null}
                //3——op=u> {"before":{"source_table":"a","sink_table":"a","sink_family":"a","sink_columns":"aaa","sink_row_key":"a"},"after":{"source_table":"a","sink_table":"a","sink_family":"a","sink_columns":"aaabbb","sink_row_key":"a"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1724687584000,"snapshot":"false","db":"gmall2024_config","sequence":null,"table":"table_process_dim","server_id":1,"gtid":null,"file":"mysql-bin.000003","pos":11426872,"row":0,"thread":71,"query":null},"op":"u","ts_ms":1724687584782,"transaction":null}
                //4——op=d> {"before":{"source_table":"a","sink_table":"a","sink_family":"a","sink_columns":"aaabbb","sink_row_key":"a"},"after":null,"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1724687619000,"snapshot":"false","db":"gmall2024_config","sequence":null,"table":"table_process_dim","server_id":1,"gtid":null,"file":"mysql-bin.000003","pos":11427233,"row":0,"thread":71,"query":null},"op":"d","ts_ms":1724687619407,"transaction":null}

                //为了处理方便 先将jsonStr转换成jsonObj
                JSONObject jsonObj = JSON.parseObject(jsonStr);
                TableProcessDwd tp = null;
                //获取操作类型
                String op = jsonObj.getString("op");
                if("d".equals(op)){
                    //对配置表进行了删除操作 需要从before属性中过去删除前配置信息
                    tp = jsonObj.getObject("before", TableProcessDwd.class);
                }else{
                    //对配置表进行了 读取 插入 更新操作 需要从after属性中获取配置信息
                    tp = jsonObj.getObject("after", TableProcessDwd.class);
                }
                tp.setOp(op);
                return tp;
            }
        });
//        tpDS.print();
        //todo 对配置流进行广播 --broadcast
        MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor
                = new MapStateDescriptor<String, TableProcessDwd>("mapStateDescriptor",String.class,TableProcessDwd.class);
        BroadcastStream<TableProcessDwd> broadcastDS = tpDS.broadcast(mapStateDescriptor);
        //todo 关联主流业务数据 和 广播流中的配置信息 --connect
        BroadcastConnectedStream<JSONObject, TableProcessDwd> connectDS = jsonObjDS.connect(broadcastDS);
        //todo 对关联后的数据进行处理 --process
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> splitDS = connectDS.process(new BaseDbTableProcessFunction(mapStateDescriptor));
//        splitDS.print();
        //todo 将处理逻辑比较简单的事实表数据写到kafka的不同主题中
        splitDS.sinkTo(FlinkSinkUtil.getKafkaSink());
    }
}
