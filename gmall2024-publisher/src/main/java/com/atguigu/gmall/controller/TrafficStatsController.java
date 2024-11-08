package com.atguigu.gmall.controller;

/*
流量与Controller
 */

import com.atguigu.gmall.bean.TrafficUvCt;
import com.atguigu.gmall.service.TrafficStatsService;
import com.atguigu.gmall.util.DateFormatUtil;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

@RestController
public class TrafficStatsController {
    @Autowired
    private TrafficStatsService trafficStatsService;

    @RequestMapping("/ch")
    public String getChUvCt(
            @RequestParam(value = "date",defaultValue = "0") Integer date,
            @RequestParam(value = "limit",defaultValue = "10") Integer limit
    ) {
        if(date==0){
            date = DateFormatUtil.now();
        }
        List<TrafficUvCt> trafficUvCtList = trafficStatsService.getChUvCt(date, limit);
        ArrayList chList = new ArrayList();
        ArrayList uvCtList = new ArrayList();

        for (TrafficUvCt trafficUvCt : trafficUvCtList) {
            chList.add(trafficUvCt.getCh());
            uvCtList.add(trafficUvCt.getUvCt());
        }

        String json = "{\"status\": 0,\"data\": {\"categories\": [\""+StringUtils.join(chList,"\",\"")+"\"],\n" +
                "    \"series\": [{\"name\": \"手机品牌\",\"data\": ["+StringUtils.join(uvCtList,",")+"]}]}}";
        return json;
    }
}
