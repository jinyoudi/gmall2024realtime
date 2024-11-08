package com.atguigu.gmall.realtime.dws.app;

/*
交易域SKU粒度下单各窗口汇总表
    维度:sku
    度量:原始金额、活动减免金额、优惠券减免金额和订单金额
    数据来源:dwd下单事实表

    分析:
        dwd下单事实表是由4张表组成,订单表、订单明细表、订单明细活动表、订单明细优惠券表。
        订单明细是主表
        和订单表进行关联的时候,使用的内连接
        和订单明细活动以及订单明细优惠券关联的时候,使用的是左外连接
        如果是左外连接,左表数据先到,右表数据后到,会产生生3条结果
            左表 null 标记为+I
            左表 null 标记为-D
            左表 右表  标记为+I
        将这样的结果发送到kafka的主题,kafka主题会接收到3条消息
            左表 null
            null
            左表 右表
        当我们从kafka主题中读取消息的时候,如果使用的是FlinkSQL的方式读取,会自动的对空消息进行处理
        如果使用的是FlinkAPI的方式读取,默认SimpleStringschema是处理不了空消息的,需要我们手动实现反序列化。

        对于第一条和第三条数据,属于重复数据,我们在DWS层中,需要做去重
            去重方式1:状态+定时器
            去重方式2:状态+抵消

        关联商品sku相关的维度（spu、tm、category）
            最基本的维度关联
            优化1：旁路缓存
            优化2：异步IO
 */

/*
sku粒度下单业务过程聚合统计
 */

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TradeSkuOrderBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.BeanToJsonStrMapFunction;
import com.atguigu.gmall.realtime.common.function.DimAsyncFunction;
import com.atguigu.gmall.realtime.common.function.DimMapFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.atguigu.gmall.realtime.common.util.RedisUtil;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

import javax.xml.bind.SchemaOutputResolver;
import java.awt.*;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

/*
sku粒度下单业务过程聚合统计
    维度：sku
    度量：原始金额 优惠券减免金额 活动减免金额 实付金额
需要启动的进程
    zk kafka maxwell hdfs hbase redis doris DwdTradeOrderDetail DwsTradeSkuOrderWindow
开发流程：
    基本环境准备
    检查点相关的设置
    从kafka的下单事实表中读取数据
    空消息的处理并将流中数据类型进行转换 jsonStr->jsonObj
    去重
        为什么会产生重复数据？
            我们是从下单事实表中读取数据的 下单事实表由订单表 订单明细表 订单明细活动 和订单明细优惠券表 四张表组成
            订单明细是主表 和订单表进行关联的时候 使用的是内连接
                        和订单明细活动以及订单明细优惠券表关联的时候 用的是左外连接
            如果左外连接的左表数据先到 右表数据后到 查询结果有3条数据
                左表 null  标记为+I
                左表 null  标记-D
                左表 右表   标记+I
            这样的数据 发送到kafka主题 kafka主题会接收到3条消息
                左表 null
                null
                左表 右表
            所以我们从下单事实表中读取数据的时候 需要去过滤空消息 进行去重
        去重前按照唯一键进行分组
        去重方案1：状态+定时器
            当第一条数据到来的时候 将数据放到状态中保存起来 并注册5s后执行的定时器
            当第二条数据到来的时候 会用第二条数据的聚合时间和第一条数据的聚合时间进行比较 将时间大的数据放在状态中
            当定时器被出发执行的时候 将状态的数据发送到下游
            优点：如果数据重复 只会往下游发送一条数据 数据不会膨胀
            缺点：时效性比较差 需要等待定时器执行
        去重方案2：状态+抵消
            当第一条数据到来的时候 直接将数据放到状态中 并且向下游传递
            当第二条数据到来的时候 将状态中影响到度量值的字段 进行取反 传递到下游
            并且将第二条数据向下游传递
            优点：时效性好
            缺点：如果出现重复，要向下游传递三条数据，数据出现膨胀
   指定watermark 以及 提取事件时间字段
   再次对流中数据进行类型转换 将jsonObj->实体类对象 （相当于wordcount封装成二元组的过程）
   按照统计的维度分组
   开窗
   聚合计算
   维度关联
        维度关联最基本的实现    HBaseUtil->getRow
        优化1：旁路缓存
            思路：先从缓存中获取维度数据 如果从缓存中获取到了维度数据（缓存命中） 直接将其作为结果进行返回
                如果在缓存中没有找到要关联的维度 发送请求到HBase中进行查询 并将查询的结果放到缓存中缓存起来 方便下次查询使用
            选型：
                状态    性能很好 维护性差
                redis  性能不错 但是维护性好  选择redis
            关于redis的配置
                key   维度表名:主键值
                type  string
                失效时间 1day  避免冷数据常驻内存 给内存带来压力
                注意：如果维度数据发生变换 需要清除缓存 DimSinkFunction->invoke
        优化2：异步IO
            为什么使用异步？
                如果在flink成需求 想要去提升某个算子的处理能力 可以去提升这个算子的并行度 但是 更多的并行度 需要更多的硬件资源 不可能无限制的提升
                所以在资源有限的情况下 考虑异步操作
            异步使用场景：
                用外部系统的数据 扩展流中的数据的时候
            默认情况下 如果使用map算子 对流中数据进行处理 底层用的同步方法处理 处理完一个元素后再处理下个元素 性能极低
            所以在维度关联的时候 使用FLink提供的发送异步请求的API 进行异步处理
            AsyncDataStream.[un]orderedWait(
                流.
                如果发送异步请求，需要实现AsyncFunction接口，
                超时时间，
                时间单位
            )
            同时还需要HbaseUtil添加异步读取数据的方法
            RedisUtil添加异步读写数据的方法
            封装了一个模板类 专门发送异步请求进行维度关联
                class DimAsyncFunction<T> extends RichAsyncFunction<T,T> implements DimJoinFunction<T>
                    asyncInvoke:
                        //创建异步编排对象，有返回值
                        CompletableFuture.supplyAsync
                        //执行线程任务，有入参 有返回值
                        .thenApplyAsync
                        //执行线程任务 有入参 无返回值
                        .thenAcceptAsync
    将数据写到Doris中
 */
public class DwsTradeSkuOrderWindow extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwsTradeSkuOrderWindow().start(
                10029,
                4,
                "dws_trade_sku_order_window",
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );

    }

    @Override
    public void hadle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDs) {
        //todo 1、过滤空消息 并且对流中的数据进行类型转换 jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDs.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        if (jsonStr != null) {
                            out.collect(JSON.parseObject(jsonStr));
                        }
                    }
                }
        );
//        jsonObjDS.print();
        //{"create_time":"2024-11-01 13:23:26","sku_num":"1","split_original_amount":"11.0000","split_coupon_amount":"0.0","sku_id":"24","date_id":"2024-11-01","user_id":"1130","province_id":"30","sku_name":"金沙河面条 原味银丝挂面 龙须面 方便速食拉面 清汤面 900g","id":"22723","order_id":"16303","split_activity_amount":"0.0","split_total_amount":"11.0","ts":1730525006}
        //todo 2、按照唯一键（订单明细的id）进行分组
        KeyedStream<JSONObject, String> orderDetailIdKeyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getString("id"));
        //todo 3、去重
        //方式1 ： 状态+定时器。  缺点：时效性变差 必须等5s 优点：如果数据重复 不会膨胀
        /*
        SingleOutputStreamOperator<JSONObject> distinctDS = orderDetailIdKeyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    private ValueState<JSONObject> lastJsonObjState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<JSONObject> valueStateDescriptor =
                                new ValueStateDescriptor<JSONObject>("lastJsonObjState", JSONObject.class);
                        lastJsonObjState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        //从状态中获取上次接收到的json对象
                        JSONObject lastJsonObj = lastJsonObjState.value();
                        if (lastJsonObj == null) {
                            //说明没有重复 将当前接收到的这条json数据放到状态中 并注册5s后执行的定时器
                            lastJsonObjState.update(jsonObj);

                            long currentProcessingTime = ctx.timerService().currentProcessingTime();
                            ctx.timerService().registerProcessingTimeTimer(currentProcessingTime + 5000L);
                        } else {
                            //说明重复了 用当前数据的聚合时间和状态中的数据聚合时间进行比较 将时间大的数据放在状态
                            //伪代码，“聚合时间戳”。
                            String lastTs = lastJsonObj.getString("聚合时间时间戳");
                            String curTs = jsonObj.getString("聚合时间戳");
                            if (curTs.compareTo(lastTs) >= 0) {
                                //将第二条数据更新到状态
                                lastJsonObjState.update(jsonObj);
                            }
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, JSONObject, JSONObject>.OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                        //当定时器被触发执行的时候 将状态中的数据发送到下游 并清除状态
                        JSONObject jsonObj = lastJsonObjState.value();
                        out.collect(jsonObj);
                        lastJsonObjState.clear();
                    }
                }
        );
         */

        //方式2 ： 状态+抵消 优点：时效性好 缺点：如果出现重复 需要向下游传递3条数据（数据膨胀）
        SingleOutputStreamOperator<JSONObject> distinctDS = orderDetailIdKeyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    private ValueState<JSONObject> lastJsonObjState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<JSONObject> valueStateDescriptor =
                                new ValueStateDescriptor<JSONObject>("lastJsonObjState", JSONObject.class);
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10)).build());
                        lastJsonObjState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        //从状态中获取上次接收到的数据
                        JSONObject lastJsonObj = lastJsonObjState.value();
                        if (lastJsonObj != null) {
                            //说明重复了，将已经发送到下游的数据，影响到度量值的字段进行取反操作 再传递到下游
                            String splitOriginalAmount = lastJsonObj.getString("split_original_amount");
                            String splitCouponAmount = lastJsonObj.getString("split_coupon_amount");
                            String splitActivityAmount = lastJsonObj.getString("split_activity_amount");
                            String splitTotalAmount = lastJsonObj.getString("split_total_amount");

                            //都在上游连接前的主表
                            lastJsonObj.put("split_original_amount", "-" + splitOriginalAmount);
                            lastJsonObj.put("split_coupon_amount", "-" + splitCouponAmount);
                            lastJsonObj.put("split_activity_amount", "-" + splitActivityAmount);
                            lastJsonObj.put("split_total_amount", "-" + splitTotalAmount);
                            out.collect(lastJsonObj);
                        }
                        lastJsonObjState.update(jsonObj);
                        out.collect(jsonObj);
                    }
                }
        );
//        distinctDS.print();
        //todo 4、指定watermark 提取事件时间字段
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = distinctDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                                        return jsonObj.getLong("ts") * 1000;
                                    }
                                }
                        )
        );
        //todo 5、再次对流中数据进行类型转换 jsonObj -> 封装成统计的实体类对象
        SingleOutputStreamOperator<TradeSkuOrderBean> beanDS = withWatermarkDS.map(
                new MapFunction<JSONObject, TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean map(JSONObject jsonObj) throws Exception {
                        //{"create_time":"2024-11-01 13:23:26","sku_num":"1","split_original_amount":"11.0000","split_coupon_amount":"0.0","sku_id":"24","date_id":"2024-11-01","user_id":"1130","province_id":"30","sku_name":"金沙河面条 原味银丝挂面 龙须面 方便速食拉面 清汤面 900g","id":"22723","order_id":"16303","split_activity_amount":"0.0","split_total_amount":"11.0","ts":1730525006}
                        String skuId = jsonObj.getString("sku_id");
                        BigDecimal splitOriginalAmount = jsonObj.getBigDecimal("split_original_amount");
                        BigDecimal splitCouponAmount = jsonObj.getBigDecimal("split_coupon_amount");
                        BigDecimal splitActivityAmount = jsonObj.getBigDecimal("split_activity_amount");
                        BigDecimal splitTotalAmount = jsonObj.getBigDecimal("split_total_amount");
                        long ts = jsonObj.getLong("ts") * 1000;

                        TradeSkuOrderBean orderBean = TradeSkuOrderBean.builder()
                                .skuId(skuId)
                                .originalAmount(splitOriginalAmount)
                                .couponReduceAmount(splitCouponAmount)
                                .activityReduceAmount(splitActivityAmount)
                                .orderAmount(splitTotalAmount)
                                .ts(ts)
                                .build();
                        return orderBean;
                    }
                }
        );
//        beanDS.print();
        //todo 6、分组
        KeyedStream<TradeSkuOrderBean, String> skuIdKeyedDS = beanDS.keyBy(TradeSkuOrderBean::getSkuId);
        //todo 7、开窗
        WindowedStream<TradeSkuOrderBean, String, TimeWindow> windowDS = skuIdKeyedDS.window(TumblingProcessingTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));
        //todo 8、聚合
        SingleOutputStreamOperator<TradeSkuOrderBean> reduceDS = windowDS.reduce(
                new ReduceFunction<TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean reduce(TradeSkuOrderBean value1, TradeSkuOrderBean value2) throws Exception {
                        //bigdecimal 用add方法加
                        value1.setOriginalAmount(value1.getOriginalAmount().add(value2.getOriginalAmount()));
                        value1.setActivityReduceAmount(value1.getActivityReduceAmount().add(value2.getActivityReduceAmount()));
                        value1.setCouponReduceAmount(value1.getCouponReduceAmount().add(value2.getCouponReduceAmount()));
                        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                        return value1;
                    }
                },
                //这里用下更底层的，也可以用windowfunction 重写的是apply方法
                new ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>.Context context, Iterable<TradeSkuOrderBean> elements, Collector<TradeSkuOrderBean> out) throws Exception {
                        TradeSkuOrderBean orderBean = elements.iterator().next();
                        TimeWindow window = context.window();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDate = DateFormatUtil.tsToDate(window.getStart());

                        orderBean.setStt(stt);
                        orderBean.setEdt(edt);
                        orderBean.setCurDate(curDate);
                        out.collect(orderBean);
                    }
                }
        );
//        reduceDS.print();
        //todo 9、关联sku（商品）维度
        //维度关联最基本的实现方式
        /*
        SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoDS = reduceDS.map(
                new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
                    //初始化连接
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
                    public TradeSkuOrderBean map(TradeSkuOrderBean orderBean) throws Exception {
                        //根据流中的对象 获取要关联的维度的主键
                        String skuId = orderBean.getSkuId();
                        //根据维度主键到hbase维度表中获取对应的维度对象
                        JSONObject skuInfoJsonObj = HBaseUtil.getRow(hbaseConn, Constant.HBASE_NAMESPACE, "dim_sku_info", skuId, JSONObject.class, false);
                        //将维度对象相关的维度属性补充到流中对象上
                        //维度信息有：id,spu_id,price,sku_name,sku_desc,weight,tm_id,category3_id,sku_default_img,is_sale,create_time
                        orderBean.setSkuName(skuInfoJsonObj.getString("sku_name"));
                        orderBean.setSpuId(skuInfoJsonObj.getString("spu_id"));
                        orderBean.setCategory3Id(skuInfoJsonObj.getString("category3_id"));
                        orderBean.setTrademarkId(skuInfoJsonObj.getString("tm_id"));
                        return orderBean;
                    }
                }
        );
         */
//        withSkuInfoDS.print();
        //优化1 旁路缓存
        /*
        SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoDS = reduceDS.map(
                new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
                    private Connection hbaseConn;
                    private Jedis jedis;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = HBaseUtil.getHbaseConnection();
                        jedis = RedisUtil.getJedis();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHbaseConnection(hbaseConn);
                        RedisUtil.closeJedis(jedis);
                    }

                    @Override
                    public TradeSkuOrderBean map(TradeSkuOrderBean orderBean) throws Exception {
                        //根据流中对象获取要关联的维度对象
                        String skuId = orderBean.getSkuId();
                        //先从redis中查询维度 根据维度主键
                        JSONObject dimJsonObj = RedisUtil.readDim(jedis, "dim_sku_info", skuId);
                        if(dimJsonObj != null){
                            //如果在redis中找到了对应的维度数据 直接作为查询结果返回
                            System.out.println("~~~~从redis中查询维度数据~~~~~~~");
                        }else{
                            //如果在redis中没有找到对应的维度数据 需要发送请求到hbase中查询对应的维度
                            System.out.println("~~~redis没有 维度从hbase中请求~~~~~~");
                            dimJsonObj = HBaseUtil.getRow(hbaseConn, Constant.HBASE_NAMESPACE, "dim_sku_info", skuId, JSONObject.class);
                            if(dimJsonObj != null){
                                //说明hbase中有维度数据，并且将查询的维度放在redis中缓存起来
                                RedisUtil.writeDim(jedis,"dim_sku_info", skuId,dimJsonObj);
                                System.out.println("~~~hbase中有数据，查询到了，同时把数据放进redis中");
                            }else{
                                System.out.println("~~~没有从hbase找到要关联的维度");
                            }
                        }
                        //将维度对象相关的维度属性补充道流中对象上
                        if(dimJsonObj != null){
                            orderBean.setSkuName(dimJsonObj.getString("sku_name"));
                            orderBean.setSpuId(dimJsonObj.getString("spu_id"));
                            orderBean.setCategory3Id(dimJsonObj.getString("category3_id"));
                            orderBean.setTrademarkId(dimJsonObj.getString("tm_id"));
                        }
                        return orderBean;
                    }
                }
        );
         */
//        withSkuInfoDS.print();

        //第三个版本 优化1：使用旁路缓存模板来关联维度
        /*
        SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoDS = reduceDS.map(
                new DimMapFunction<TradeSkuOrderBean>() {
                    @Override
                    public void addDims(TradeSkuOrderBean orderBean, JSONObject dimJsonObj) {
                        orderBean.setSkuName(dimJsonObj.getString("sku_name"));
                        orderBean.setSpuId(dimJsonObj.getString("spu_id"));
                        orderBean.setCategory3Id(dimJsonObj.getString("category3_id"));
                        orderBean.setTrademarkId(dimJsonObj.getString("tm_id"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_sku_info";
                    }

                    @Override
                    public String getRowKey(TradeSkuOrderBean orderBean) {
                        return orderBean.getSkuId();
                    }
                }
        );
//        withSkuInfoDS.print();

         */

        /*
        //优化2
        SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoDS = AsyncDataStream.unorderedWait(
                //当前操作无所谓顺序 所以unorder
                reduceDS,
                //如何发送异步请求，实现分发请求的AsyncFunction
                new RichAsyncFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
                    private AsyncConnection hbaseAsyncConn;
                    private StatefulRedisConnection<String,String> redisAsyncConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseAsyncConn = HBaseUtil.getHBaseAsyncConnection();
                        redisAsyncConn = RedisUtil.getRedisAsyncConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeAsyncHbaseConnection(hbaseAsyncConn);
                        RedisUtil.closeRedisAsyncConnection(redisAsyncConn);
                    }

                    @Override
                    public void asyncInvoke(TradeSkuOrderBean orderBean, ResultFuture<TradeSkuOrderBean> resultFuture) throws Exception {
                        //根据当前流中对象获取要关联的维度的主键
                        String skuId = orderBean.getSkuId();
                        //根据维度的主键到redis中获取维度数据
                        JSONObject dimJsonObj = RedisUtil.readDimAsync(redisAsyncConn, "dim_sku_info", skuId);
                        if(dimJsonObj != null){
                            //如果在redis中找到了要关联的维度(缓存命中)，直接将命中的维度作为结果返回即可
                            System.out.println("~~~~从redis中获取维度数据~~~~");
                        }else {
                            //如果在redis中没有找到要关联的维度，发送请求到hbase中查找
                            dimJsonObj = HBaseUtil.readDimAsync(hbaseAsyncConn, Constant.HBASE_NAMESPACE, "dim_sku_info", skuId);
                            if(dimJsonObj!=null){
                                System.out.println("~~~在hbase中获取维度数据~~~");
                                //将查找的维度数据写入redis缓存起来
                                RedisUtil.writeDimAsync(redisAsyncConn,"dim_sku_info",skuId,dimJsonObj);
                            }else{
                                System.out.println("~~~=没有在hbase中获取维度数据~~~");
                            }
                        }
                        //将维度对象相关的维度属性补充到流中对象上
                        if(dimJsonObj != null){
                            orderBean.setSkuName(dimJsonObj.getString("sku_name"));
                            orderBean.setSpuId(dimJsonObj.getString("spu_id"));
                            orderBean.setCategory3Id(dimJsonObj.getString("category3_id"));
                            orderBean.setTrademarkId(dimJsonObj.getString("tm_id"));
                        }
                        //获取数据库交互的结果并发送给ResultFuture的回调函数 对关联后的数据传递到下游
                        resultFuture.complete(Collections.singleton(orderBean));
                    }
                },
                60,
                TimeUnit.SECONDS
        );
//        withSkuInfoDS.print();
         */

        //发送异步io + 模板
        SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoDS = AsyncDataStream.unorderedWait(
                reduceDS,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public void addDims(TradeSkuOrderBean orderBean, JSONObject dimJsonObj) {
                        orderBean.setSkuName(dimJsonObj.getString("sku_name"));
                        orderBean.setSpuId(dimJsonObj.getString("spu_id"));
                        orderBean.setCategory3Id(dimJsonObj.getString("category3_id"));
                        orderBean.setTrademarkId(dimJsonObj.getString("tm_id"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_sku_info";
                    }

                    @Override
                    public String getRowKey(TradeSkuOrderBean orderBean) {
                        return orderBean.getSkuId();
                    }
                },
                60,
                TimeUnit.SECONDS
        );
//        withSkuInfoDS.print();

        //todo 10、关联spu
        SingleOutputStreamOperator<TradeSkuOrderBean> withSpuInfoDS = AsyncDataStream.unorderedWait(
                withSkuInfoDS,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public void addDims(TradeSkuOrderBean orderBean, JSONObject dimJsonObj) {
                        orderBean.setSpuName(dimJsonObj.getString("spu_name"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_spu_info";
                    }

                    @Override
                    public String getRowKey(TradeSkuOrderBean orderBean) {
                        return orderBean.getSpuId();
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        //todo 11、关联tm维度
        SingleOutputStreamOperator<TradeSkuOrderBean> withTmDS = AsyncDataStream.unorderedWait(
                withSpuInfoDS,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public void addDims(TradeSkuOrderBean orderBean, JSONObject dimJsonObj) {
                        orderBean.setTrademarkName(dimJsonObj.getString("tm_name"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_trademark";
                    }

                    @Override
                    public String getRowKey(TradeSkuOrderBean orderBean) {
                        return orderBean.getTrademarkId();
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        //todo 12、关联category3维度
        SingleOutputStreamOperator<TradeSkuOrderBean> c3Stream = AsyncDataStream.unorderedWait(
                withTmDS,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getRowKey(TradeSkuOrderBean bean) {
                        return bean.getCategory3Id();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category3";
                    }

                    @Override
                    public void addDims(TradeSkuOrderBean bean, JSONObject dim) {
                        bean.setCategory3Name(dim.getString("name"));
                        bean.setCategory2Id(dim.getString("category2_id"));
                    }
                },
                120,
                TimeUnit.SECONDS
        );

        //todo 13、关联category2维度
        SingleOutputStreamOperator<TradeSkuOrderBean> c2Stream = AsyncDataStream.unorderedWait(
                c3Stream,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getRowKey(TradeSkuOrderBean bean) {
                        return bean.getCategory2Id();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category2";
                    }

                    @Override
                    public void addDims(TradeSkuOrderBean bean, JSONObject dim) {
                        bean.setCategory2Name(dim.getString("name"));
                        bean.setCategory1Id(dim.getString("category1_id"));
                    }
                },
                120,
                TimeUnit.SECONDS
        );

        //todo 14、关联category1维度
        SingleOutputStreamOperator<TradeSkuOrderBean> withC1DS = AsyncDataStream.unorderedWait(
                c2Stream,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getRowKey(TradeSkuOrderBean bean) {
                        return bean.getCategory1Id();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category1";
                    }

                    @Override
                    public void addDims(TradeSkuOrderBean bean, JSONObject dim) {
                        bean.setCategory1Name(dim.getString("name"));
                    }
                },
                120,
                TimeUnit.SECONDS
        );
//        withC1DS.print();

        //todo 15、将关联的结果写到Doris表中
        withC1DS
                .map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_sku_order_window"));
    }
}
