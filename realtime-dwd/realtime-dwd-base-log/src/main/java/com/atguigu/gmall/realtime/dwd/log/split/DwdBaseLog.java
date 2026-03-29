package com.atguigu.gmall.realtime.dwd.log.split;


/*
11.2 流量域未经加工的事务事实表（日志分流）
需要启动的进程
    zk,kafka,flume

日志数据格式
{"common":
    {"ar":"2","ba":"vivo","ch":"xiaomi","is_new":"1","md":"vivo x90","mid":"mid_64","os":"Android 13.0","sid":"336a2af9-4394-4310-bdab-9ef9cc35b80e","uid":"376","vc":"v2.1.111"}
  ,"page":
    {"during_time":8822,"item":"1101","item_type":"order_id","last_page_id":"order","page_id":"payment"}
  ,"ts":1654668567000
}
 */

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import com.google.gson.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * 需要启动的进程是什么
 * zk kafka flume DwdBaseLog
 *
 *
 *
 * KafkaSource：从kafka主题中读取数据
 *              可以保证消费的精准一次，通过手动维护偏移量
 * KafkaSink：向kafka主题中写入数据（但是在本次项目学习阶段，不开启一致性了）
 *              可以保证写入的精准一次，需要做到如下几点
 *                  1、开启检查点
 *                  2、在创建kafkasink的时候.setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
 *                  3、设置事务id的前缀
 *                      .setTransactionalIdPrefix("dwd_base_log_")
 *                  4、设置事务超时时间
 *                      .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,15*60*1000+"")
 *                  5、在消费端设置消费的隔离级别为读已提交
 *                      写在了flinksourceUtil里了 .setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG,"read_committed") //读已提交
 */
public class DwdBaseLog extends BaseApp {

    private final String START = "start";
    private final String ERR = "err";
    private final String DISPLAY = "display";
    private final String ACTION = "action";
    private final String PAGE = "page";


    public static void main(String[] args) throws Exception {
        new DwdBaseLog().start(10011,4,"dwd_base_log", Constant.TOPIC_LOG);
    }
    @Override
    public void hadle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDs) {
        //todo 对流中数据类型进行转换 并做简单的etl
        SingleOutputStreamOperator<JSONObject> jsonObjDS = etl(kafkaStrDs);

        /*
        {"common":{"ar":"2","ba":"vivo","ch":"xiaomi","is_new":"1","md":"vivo x90","mid":"mid_64","os":"Android 13.0","sid":"336a2a.....","uid":"376","vc":"v2.1.111"},"page":{"during_time":8822,"item":"1101","item_type":"orderid","last_page_id":"order","page_id":"payment"},"ts":1654668567000}
         */
        //todo 对新老访客的标记"is_new"进行修复
        SingleOutputStreamOperator<JSONObject> fixedDS = fixedNewAndOld(jsonObjDS);

        //todo 分流 错误日志-错误侧输出流 启动日志-启动测输出流 曝光日志-曝光侧输出流   动作日志-动作侧输出流 页面日志-主流
        Map<String, DataStream<String>> streamMap = splitStream(fixedDS);


        //todo 将不同流的数据写到kafka的不同主题中
        writeToKafka(streamMap);


    }

    private void writeToKafka(Map<String, DataStream<String>> streamMap) {
        //        pageDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
//        errDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
//        startDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
//        displayDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
//        actionDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));
        streamMap
                .get(PAGE)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
        streamMap
                .get(ERR)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
        streamMap
                .get(START)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
        streamMap
                .get(DISPLAY)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
        streamMap
                .get(ACTION)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));

        //结果展示
        //启动 {"common":{"ar":"19","os":"Android 13.0","ch":"xiaomi","is_new":"1","md":"xiaomi 13","mid":"mid_430","vc":"v2.1.134","ba":"xiaomi","sid":"7985e738-c7cc-4e1a-b87f-6eb9c143f152"},"start":{"entry":"icon","open_ad_skip_ms":3008,"open_ad_ms":9659,"loading_time":12982,"open_ad_id":8},"ts":1716483454000}
        //错误 {"common":{"ar":"17","uid":"389","os":"Android 11.0","ch":"xiaomi","is_new":"0","md":"vivo IQOO Z6x ","mid":"mid_291","vc":"v2.1.134","ba":"vivo","sid":"a5becb08-9276-4bfb-91bd-c11558d39043"},"err":{"msg":" Exception in thread \\  java.net.SocketTimeoutException\\n \\tat com.atgugu.gmall2020.mock.bean.AppError.main(AppError.java:xxxxxx)","error_code":3921},"page":{"page_id":"order","item":"14","during_time":13604,"item_type":"sku_ids","last_page_id":"cart"},"ts":1716483455000}
        //动作 {"common":{"ar":"34","uid":"387","os":"Android 10.1","ch":"xiaomi","is_new":"1","md":"SAMSUNG Galaxy s22","mid":"mid_422","vc":"v2.1.134","ba":"SAMSUNG","sid":"68e439c9-f0a8-464b-83e2-ece9c5a210a9"},"action":{"item":"24","action_id":"cart_add","item_type":"sku_id","ts":1716483454000},"page":{"page_id":"good_detail","item":"24","during_time":16526,"item_type":"sku_id","last_page_id":"register"}}
        //曝光 {"common":{"ar":"31","uid":"354","os":"Android 13.0","ch":"vivo","is_new":"1","md":"xiaomi 13 Pro ","mid":"mid_387","vc":"v2.1.134","ba":"xiaomi","sid":"67aea7f8-83af-4eeb-973b-53a8504aa6af"},"display":{"pos_seq":9,"item":"8","item_type":"sku_id","pos_id":4},"page":{"from_pos_seq":3,"page_id":"good_detail","item":"18","during_time":5664,"item_type":"sku_id","last_page_id":"good_detail","from_pos_id":4},"ts":1716483453000}
        //页面 {"common":{"ar":"9","uid":"138","os":"iOS 13.3.1","ch":"Appstore","is_new":"1","md":"iPhone 14 Plus","mid":"mid_448","vc":"v2.1.134","ba":"iPhone","sid":"3ca1eb15-17c2-4040-99bc-9baa31a273bf"},"page":{"page_id":"home","refer_id":"4","during_time":10085},"ts":1716483454000}
    }

    private Map<String, DataStream<String>> splitStream(SingleOutputStreamOperator<JSONObject> fixedDS) {
        //定义测输出流标签
        OutputTag<String> errTag = new OutputTag<String>("errTag"){};
        OutputTag<String> startTag = new OutputTag<String>("startTag"){};
        OutputTag<String> displayTag = new OutputTag<String>("displayTag"){};
        OutputTag<String> actionTag = new OutputTag<String>("actionTag"){};
        //分流
        SingleOutputStreamOperator<String> pageDS = fixedDS.process(
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject jsonObj, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) throws Exception {
                        //错误日志
                        JSONObject errJsonObj = jsonObj.getJSONObject("err");
                        if (errJsonObj != null) {
                            //将错误日志写到错误侧输出流
                            ctx.output(errTag, jsonObj.toJSONString());
                            jsonObj.remove("err"); //虽然是错误日志，但有可能有曝光页面
                        }

                        JSONObject startJsonObj = jsonObj.getJSONObject("start");
                        if (startJsonObj != null) {
                            //启动日志
                            //将启动日志写到启动侧输出流
                            ctx.output(startTag, jsonObj.toJSONString());
                        } else {
                            //页面日志
                            JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                            JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                            Long ts = jsonObj.getLong("ts");

                            //曝光日志,displays如果有的话是个数组
                            JSONArray displayArr = jsonObj.getJSONArray("displays");
                            if (displayArr != null && displayArr.size() > 0) {
                                //遍历当前页面的所有曝光信息
                                for (int i = 0; i < displayArr.size(); i++) {
                                    JSONObject displayJsonobj = displayArr.getJSONObject(i);
                                    //定义一个新的JSON对象用于封装遍历出来的曝光数据
                                    JSONObject newDisplayJsonObj = new JSONObject();
                                    newDisplayJsonObj.put("common", commonJsonObj);
                                    newDisplayJsonObj.put("page", pageJsonObj);
                                    newDisplayJsonObj.put("display", displayJsonobj);
                                    newDisplayJsonObj.put("ts", ts);
                                    //将曝光日志写到曝光测输出流
                                    ctx.output(displayTag, newDisplayJsonObj.toJSONString());
                                }
                                jsonObj.remove("displays");
                            }

                            //动作日志
                            JSONArray actionArr = jsonObj.getJSONArray("actions");
                            if (actionArr != null && actionArr.size() > 0) {
                                //遍历每一个动作
                                for (int i = 0; i < actionArr.size(); i++) {
                                    JSONObject actionJsonobj = actionArr.getJSONObject(i);
                                    JSONObject newActionJsonObj = new JSONObject();
                                    newActionJsonObj.put("common", commonJsonObj);
                                    newActionJsonObj.put("page", pageJsonObj);
                                    newActionJsonObj.put("action", actionJsonobj);
                                    //将动作日志写到动作侧输出流
                                    ctx.output(actionTag, newActionJsonObj.toJSONString());
                                }
                                jsonObj.remove("actions");
                            }

                            //页面日志 写到主流中
                            out.collect(jsonObj.toJSONString());
                        }
                    }
                }
        );

        SideOutputDataStream<String> errDS = pageDS.getSideOutput(errTag);
        SideOutputDataStream<String> startDS = pageDS.getSideOutput(startTag);
        SideOutputDataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        SideOutputDataStream<String> actionDS = pageDS.getSideOutput(actionTag);
        pageDS.print("页面：");
        errDS.print("错误：");
        startDS.print("启动：");
        displayDS.print("曝光：");
        actionDS.print("动作：");
        Map<String, DataStream<String>> streamMap = new HashMap<>();
        streamMap.put(ERR,errDS);
        streamMap.put(START,startDS);
        streamMap.put(DISPLAY,displayDS);
        streamMap.put(ACTION,actionDS);
        streamMap.put(PAGE,pageDS);
        return streamMap;
    }

    private SingleOutputStreamOperator<JSONObject> fixedNewAndOld(SingleOutputStreamOperator<JSONObject> jsonObjDS) {
        //按照设备id进行分组
        KeyedStream<JSONObject, String> keyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        //使用flink的状态编程完成修复
        SingleOutputStreamOperator<JSONObject> fixedDS = keyedDS.map(
                new RichMapFunction<JSONObject, JSONObject>() {
                    private ValueState<String> lastVisitDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<String>("lastVisitDateState", String.class);
//                        //设置状态失效时间和类型，OnCreateAndWrite创建和写的时候才会失效
//                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10))
//                                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
//                                .build());
                        lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObj) throws Exception {
                        //获取is_new的值
                        String isNew = jsonObj.getJSONObject("common").getString("is_new");
                        //从状态中获取首次访问日期
                        String lastVisitDate = lastVisitDateState.value();
                        //获取当前访问日期
                        Long ts = jsonObj.getLong("ts");
                        String curVisitDate = DateFormatUtil.tsToDate(ts);

                        //① 如果is_new的值为1
                        if ("1".equals(isNew)) {
                            if (StringUtils.isEmpty(lastVisitDate)) {
                                //	如果键控状态为null，认为本次是该访客首次访问 APP，将日志中 ts 对应的日期更新到状态中，不对 is_new 字段做修改；
                                lastVisitDateState.update(curVisitDate);
                            } else {
                                //	如果键控状态不为null，且首次访问日期不是当日，说明访问的是老访客，将 is_new 字段置为 0；
                                if (!lastVisitDate.equals(curVisitDate)) {
                                    isNew = "0";
                                    jsonObj.getJSONObject("common").put("is_new", isNew);
                                }
                                //	如果键控状态不为 null，且首次访问日期是当日，说明访问的是新访客，不做操作；
                            }
                        } else {
                            //② 如果 is_new 的值为 0
                            //	如果键控状态为 null，说明访问 APP 的是老访客但本次是该访客的页面日志首次进入程序。当前端新老访客状态标记丢失时，日志进入程序被判定为新访客，Flink 程序就可以纠正被误判的访客状态标记，只要将状态中的日期设置为今天之前即可。本程序选择将状态更新为昨日；
                            if (StringUtils.isEmpty(lastVisitDate)) {
                                String yesterDay = DateFormatUtil.tsToDate(ts - 24 * 60 * 60 * 1000);
                                lastVisitDateState.update(yesterDay);
                            }
                            //	如果键控状态不为 null，说明程序已经维护了首次访问日期，不做操作。
                        }
                        return jsonObj;
                    }
                }
        );
//        fixedDS.print();
        return fixedDS;
    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> kafkaStrDs) {
        //定义测输出流标签
        OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag"){};


        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDs.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            //如果转换过程中 没有发生异常 说明是标准的json  把数据传送到下游
                            out.collect(jsonObj);
                        } catch (Exception e) {
                            //如果转换过程中发生了异常 说明不是标准的json 是脏数据 把数据放到侧输出流中
                            ctx.output(dirtyTag, jsonStr);
                        }
                    }
                }
        );
        jsonObjDS.print("标准的json：");
        SideOutputDataStream<String> dirtyDS = jsonObjDS.getSideOutput(dirtyTag);
        dirtyDS.print("脏数据：");

        //ETL
        //将侧输出流中的脏数据写到kafka主题中
        KafkaSink<String> kafkaSink = FlinkSinkUtil.getKafkaSink("dirty_data");
        dirtyDS.sinkTo(kafkaSink);
        return jsonObjDS;
    }
}
