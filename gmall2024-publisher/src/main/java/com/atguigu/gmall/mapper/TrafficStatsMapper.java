package com.atguigu.gmall.mapper;

/*
 */

import com.atguigu.gmall.bean.TrafficUvCt;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface TrafficStatsMapper {
    //获取某天各个渠道访客数
    @Select("select ch,sum(uv_ct) uv_ct from dws_traffic_vc_ch_ar_is_new_page_view_window partition par#{date} " +
            "group by ch ORDER BY uv_ct desc limit #{limit}")
    List<TrafficUvCt> selectChUvCt(@Param("date") Integer date,@Param("limit") Integer limit);
}
