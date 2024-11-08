package com.atguigu.gmall.controller;

/*
交易域统计Controller
 */


import com.atguigu.gmall.bean.TradeProvinceOrderAmount;
import com.atguigu.gmall.service.TradeStatsService;
import com.atguigu.gmall.util.DateFormatUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.List;

@RestController
public class TradeStatsController {
    @Autowired
    private TradeStatsService tradeStatsService;

    @RequestMapping("/gmv")
    public String getGMV(@RequestParam(value="date",defaultValue="0") Integer date){
        if(date==0){
            //说明请求的时候 没有传递日期参数 将当天日期作为查询日期
            date = DateFormatUtil.now();
        }
        BigDecimal gmv = tradeStatsService.getGMV(date);
        String json = "{\n" +
                "\"status\":0,\n" +
                "\"data\":"+gmv+"\n" +
                "}";
        return json;
    }

    @RequestMapping("/province")
    public String getProvinceAmount(@RequestParam(value = "date",defaultValue = "0") Integer date){
        if(date==0){
            //说明请求的时候 没有传递日期参数 将当天日期作为查询日期
            date = DateFormatUtil.now();
        }
        List<TradeProvinceOrderAmount> provinceOrderAmountList = tradeStatsService.getProvinceAmount(date);

        StringBuilder jsonB = new StringBuilder("{\"status\": 0,\"data\": {\"mapData\": [");
        for (int i = 0; i < provinceOrderAmountList.size(); i++) {
            TradeProvinceOrderAmount provinceOrderAmount = provinceOrderAmountList.get(i);
            jsonB.append("{\"name\": \""+provinceOrderAmount.getProvinceName()+"\",\"value\": "+provinceOrderAmount.getOrderAmount()+"}");
            if(i<provinceOrderAmountList.size()-1){
                jsonB.append(",");
            }
        }
        jsonB.append("],\"valueName\": \"交易额\"}}");
        return jsonB.toString();
    }
}
