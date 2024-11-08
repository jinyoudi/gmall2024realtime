package com.atguigu.gmall.realtime.common.function;

/*
发送异步请求进行维度关联的模板类
 */

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.DimJoinFunction;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.atguigu.gmall.realtime.common.util.RedisUtil;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.client.AsyncConnection;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T,T> implements DimJoinFunction<T> {
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
    public void asyncInvoke(T obj, ResultFuture<T> resultFuture) throws Exception {
        //创建异步编排对象 把当前多个线程任务编排管理起来
        //执行线程任务 有返回值
        CompletableFuture.supplyAsync(
                new Supplier<JSONObject>() {
                    @Override
                    public JSONObject get() {
                        //根据当前流中对象获取要关联的维度的主键
                        String key = getRowKey(obj);
                        //根据维度的主键到redis中获取维度数据
                        JSONObject dimJsonObj = RedisUtil.readDimAsync(redisAsyncConn, getTableName(), key);
                        return dimJsonObj;
                    }
                }
                //有入参 有返回值 。上一个线程任务的返回值 作为当前线程任务的入参
        ).thenApplyAsync(
                new Function<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject apply(JSONObject dimJsonObj) {
                        if(dimJsonObj !=null){
                            //如果在redis中找到了要关联的维度(缓存命中)，直接将命中的维度作为结果返回即可

                            System.out.println("~~~~从redis中找到了" + getTableName() + "表的" + getRowKey(obj) + "数据~~~");

                        }else{
                            //如果在redis中没有找到要关联的维度，发送请求到hbase中查找
                            dimJsonObj = HBaseUtil.readDimAsync(hbaseAsyncConn, Constant.HBASE_NAMESPACE, getTableName(), getRowKey(obj));
                            if(dimJsonObj != null){
                                //将查找的维度数据写入redis缓存起来

                                System.out.println("~~~~从hbase中找到了" + getTableName() + "表的" + getRowKey(obj) + "数据~~~");
                                RedisUtil.writeDimAsync(redisAsyncConn,getTableName(),getRowKey(obj),dimJsonObj);
                            }else{
                                System.out.println("~~~~没有从hbase中找到了" + getTableName() + "表的" + getRowKey(obj) + "数据~~~");
                            }
                        }
                        return dimJsonObj;
                    }
                }
                //唷入参 无返回值
        ).thenAcceptAsync(
                new Consumer<JSONObject>() {
                    @Override
                    public void accept(JSONObject dimJsonObj) {
                        if(dimJsonObj !=null){
                            //将维度对象相关的维度属性补充到流中对象上
                            addDims(obj,dimJsonObj);
                        }
                        //获取数据库交互的结果并发送给ResultFuture的回调函数 对关联后的数据传递到下游
                        resultFuture.complete(Collections.singleton(obj)); //集合的辅助类将对象转换成集合
                    }
                }
        );

    }
}
