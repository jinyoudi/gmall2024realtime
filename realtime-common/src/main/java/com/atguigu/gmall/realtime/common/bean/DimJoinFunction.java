package com.atguigu.gmall.realtime.common.bean;

/*
维度关联需要实现的接口
 */

import com.alibaba.fastjson.JSONObject;

public interface DimJoinFunction<T> {
    //把维度数据写到对应流上
    void addDims(T obj, JSONObject dimJsonObj);

    String getTableName();

    String getRowKey(T obj);
}
