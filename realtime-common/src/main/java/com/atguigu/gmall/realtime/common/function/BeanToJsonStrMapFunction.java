package com.atguigu.gmall.realtime.common.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.atguigu.gmall.realtime.common.bean.TrafficPageViewBean;
import org.apache.flink.api.common.functions.MapFunction;


/*
将流中对象转换为json格式字符串
 */

public class BeanToJsonStrMapFunction<T> implements MapFunction<T, String> {
    @Override
    public String map(T bean) throws Exception {
        SerializeConfig config = new SerializeConfig();
        config.setPropertyNamingStrategy(PropertyNamingStrategy.SnakeCase); //驼峰会转换为蛇形 蛇形的话就不会转换了
        return JSON.toJSONString(bean, config);
    }
}
