package com.atguigu.gmall.service;

/*
流量与service接口
 */

import com.atguigu.gmall.bean.TrafficUvCt;

import java.util.List;

public interface TrafficStatsService {
    //获取某天各个渠道独立访客数
    List<TrafficUvCt> getChUvCt(Integer date,Integer limit);
}
