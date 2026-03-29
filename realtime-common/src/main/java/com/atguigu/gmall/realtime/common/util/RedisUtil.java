package com.atguigu.gmall.realtime.common.util;

/*
操作redis工具类
 */

/*
旁路缓存思路
    先从缓存中获取唯独数据 如果获取到了 （缓存命中） 直接将缓存中的维度返回 如果在缓存中妹有找到要关联的维度 发送请求到hbase中查询维度
    并将查询的结果放到缓存中缓存起来 方便下次查询使用
  选型
    装填 ： 性能哼好 维护性差
    redis ： 性能不错 维护性好 本项目选择
  关于redis 的一些设置
    key ： 唯独表明:主键值
    type ： string
    expire ：1day 避免冷数据常驻内存 给内存带来压力
    注意 ： 如果维度数据发生了变化 需要将缓存的而数据清除掉
 */

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.concurrent.ExecutionException;

public class RedisUtil {

    private static JedisPool jedisPool;
    //类加载的时候就创建Jedis，用静态代码块
    static {

        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMinIdle(5); //最小空闲连接
        jedisPoolConfig.setMaxTotal(100);
        jedisPoolConfig.setMaxIdle(5); //
        jedisPoolConfig.setBlockWhenExhausted(true); //资源等待
        jedisPoolConfig.setMaxWaitMillis(2000);
        jedisPoolConfig.setTestOnBorrow(true); //每次去池子里连接 做个测试 目的为了连接是否正常

        jedisPool = new JedisPool(jedisPoolConfig,"hadoop102",6379,10000); //超时时间是ms
    }

    //获取Jedis getJedis()
    public static Jedis getJedis(){
        System.out.println("~~~~~~~~~~~~获取jedis客户端~~~~~~~~~~~~~~~");
        Jedis jedis = jedisPool.getResource();
        return jedis;
    }

    //关闭Jedis closeJedis(Jedis jedis)
    public static void  closeJedis(Jedis jedis){
        System.out.println("~~~~~~~~~~~~关闭jedis客户端~~~~~~~~~~~~~~~");
        if(jedis != null){
            jedis.close();
        }
    }

    //获取异步操作redis的连接对象
    public static StatefulRedisConnection<String,String> getRedisAsyncConnection(){
        System.out.println("~~~获取异步redis客户端~~~");
        RedisClient redisClient = RedisClient.create("redis://hadoop102:6379/0");
        return redisClient.connect();
    }

    //关闭异步操作redis的连接对象
    public static void closeRedisAsyncConnection(StatefulRedisConnection<String,String> asyncRedisConn) {
        System.out.println("~~~关闭异步redis客户端~~~");
        if(asyncRedisConn !=null && asyncRedisConn.isOpen()){
            asyncRedisConn.close();
        }
    }


    //异步的方式从redis中取数据
    public static JSONObject readDimAsync(StatefulRedisConnection<String,String> asyncRedisConn,String tableName,String id){
        RedisAsyncCommands<String, String> asyncCommands = asyncRedisConn.async();
        String key = getKey(tableName, id);
        try {
            String dimJsonStr = asyncCommands.get(key).get(); //需要两层get
            if(StringUtils.isNotEmpty(dimJsonStr)){
                //说明缓存命中
                JSONObject jsonObject = JSON.parseObject(dimJsonStr);
                return jsonObject;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    //异步的方式从redis中放数据
    public static void writeDimAsync(StatefulRedisConnection<String,String> asyncRedisConn,String tableName,String id ,JSONObject dimJsonObj){
        RedisAsyncCommands<String, String> asyncCommonds = asyncRedisConn.async();
        String key = getKey(tableName, id);
        asyncCommonds.setex(key,24*60*60,dimJsonObj.toJSONString());
    }


    //从redis中取数据
    public static JSONObject readDim(Jedis jedis,String tableName,String id){
        //拼接key
        String key = getKey(tableName, id);
        //根据key到redis中获取唯独数据
        String dimJsonStr = jedis.get(key);
        if(StringUtils.isNotEmpty(dimJsonStr)){
            JSONObject dimJsonObj = JSON.parseObject(dimJsonStr);
            return dimJsonObj;
        }

        return null;
    }

    public static String getKey(String tableName, String id) {
        String key = tableName + ":" + id;
        return key;
    }

    //向redis中放数据
    public static void writeDim(Jedis jedis,String tableName,String id ,JSONObject dimJsonObj){
        String key = getKey(tableName, id);
        jedis.setex(key,24*60*60,dimJsonObj.toJSONString());  //带失效时间的,单位s
    }

//    public static void main(String[] args) {
//        Jedis jedis = getJedis();
//        String pong = jedis.ping();
//        System.out.println(pong);
//        closeJedis(jedis);
//    }

}
