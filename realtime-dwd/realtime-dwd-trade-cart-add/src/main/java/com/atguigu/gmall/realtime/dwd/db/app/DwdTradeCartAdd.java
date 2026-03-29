package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/*
加购事实表
 */
public class DwdTradeCartAdd extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeCartAdd().start(
                10013,
                4,
                Constant.TOPIC_DWD_TRADE_CART_ADD
        );
    }

    @Override
    public void hadle(StreamTableEnvironment tableEnv) {
        //todo 从kafka的topic_db主题中读取数据 创建动态表
        readOdsDb(tableEnv,Constant.TOPIC_DWD_TRADE_CART_ADD);
        //todo 过滤出加购数据 table="cart_info" type="insert"、 type="update"并且修改的是加购商品的数量，修改后的值大于修改前的值
        Table cartInfo = tableEnv.sqlQuery("select\n" +
                "    `data`['id'] as id,\n" +
                "    `data`['user_id'] as user_id,\n" +
                "    `data`['sku_id'] as sku_id,\n" +
                "    if(type='insert',`data`['sku_num'],cast((cast(`data`['sku_num'] as int)-cast(`old`['sku_num'] as int)) as string) ) as sku_num,\n" +
                "    ts\n" +
                "from topic_db\n" +
                "where `table` = 'cart_info'\n" +
                "and (\n" +
                "    type = 'insert'\n" +
                "    or \n" +
                "    (type = 'update' and `old`['sku_num'] is not null and cast(`data`['sku_num'] as int)>cast(`old`['sku_num'] as int))\n" +
                ")");
//        cartInfo.execute().print();
        //todo 将过滤出来的加购数据写出到kafka主题中
        //创建动态表和要写入的主题进行映射
        tableEnv.executeSql("create table "+Constant.TOPIC_DWD_TRADE_CART_ADD+"(\n" +
                "    id string,\n" +
                "    user_id string,\n" +
                "    sku_id string,\n" +
                "    sku_num string,\n" +
                "    ts bigint,\n" +
                "    PRIMARY KEY(id) NOT ENFORCED\n" +
                ")" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_CART_ADD));
        //写入
        cartInfo.executeInsert(Constant.TOPIC_DWD_TRADE_CART_ADD);
    }
}
