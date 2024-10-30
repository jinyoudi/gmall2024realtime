package com.atguigu.gmall.realtime.dws.app;

/*
搜索关键词聚合统计

需要启动的进程
    zk kafka flume doris DwdBaseLog DwsTrafficSourceKeywordPageViewWindow
 */

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import com.atguigu.gmall.realtime.dws.function.KeywordUDTF;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

//检查点要开启
public class DwsTrafficSourceKeywordPageViewWindow extends BaseSQLApp {

    public static void main(String[] args) {
        new DwsTrafficSourceKeywordPageViewWindow().start(
                10021,
                4,
                "dws_traffic_source_keyword_page_view_window"
        );
    }

    @Override
    public void hadle(StreamTableEnvironment tableEnv) {
        //todo 注册自定义函数 到 表执行环境中
        tableEnv.createTemporarySystemFunction("ik_analyze", KeywordUDTF.class);
        //todo 从页面日志事实表中读取数据，创建动态表 并指定watermark的生成策略以及提取事件时间字段
        tableEnv.executeSql("create table page_log(\n" +
                " common map<string, string>, \n" +
                " page map<string, string>, \n" +
                " ts bigint, \n" +
                " et as to_timestamp_ltz(ts, 3), \n" +
                " watermark for et as et \n" +
                ")" + SQLUtil.getKafkaDDL(Constant.TOPIC_DWD_TRAFFIC_PAGE,"dws_traffic_source_keyword_page_view_window"));
//        tableEnv.executeSql("select * from page_log").print();
        //todo 过滤出搜索行为
        Table searchTable = tableEnv.sqlQuery("select \n" +
                "page['item'] fullword, \n" +
                "et \n" +
                "from page_log \n" +
                "where ( page['last_page_id'] ='search' " +
                "        or page['last_page_id'] ='home' " +
                "       )\n" +
                "and page['item_type']='keyword' \n" +
                "and page['item'] is not null \n");
//        searchTable.execute().print();
        tableEnv.createTemporaryView("searchTable", searchTable);

        //todo 调用自定义函数完成分词操作 并和原表的其他字段进行join
        Table splitTable = tableEnv.sqlQuery("select keyword,et\n" +
                "        from searchTable,\n" +
                "        lateral table(ik_analyze(fullword)) t(keyword)");
        tableEnv.createTemporaryView("split_table",splitTable);
//        tableEnv.executeSql("select * from split_table").print();
        //todo 分组 开窗 聚合
        Table resTable = tableEnv.sqlQuery("select " +
                " date_format(window_start, 'yyyy-MM-dd HH:mm:ss') stt, " +
                " date_format(window_end, 'yyyy-MM-dd HH:mm:ss') edt, " +
                " date_format(window_start, 'yyyyMMdd') cur_date, " +
                " keyword," +
                " count(*) keyword_count " +
                "from table( tumble(table split_table, descriptor(et), interval '10' second ) ) " +
                "group by window_start, window_end, keyword ");
//        resTable.execute().print();
        //todo 将聚合结果写到doris中
        tableEnv.executeSql("create table dws_traffic_source_keyword_page_view_window(" +
                "  stt string, " +  // 2023-07-11 14:14:14
                "  edt string, " +
                "  cur_date string, " +
                "  keyword string, " +
                "  keyword_count bigint " +
                ")with(" +
                " 'connector' = 'doris'," +
                " 'fenodes' = '" + Constant.DORIS_FE_NODES + "'," +
                "  'table.identifier' = '" + Constant.DORIS_DATABASE + ".dws_traffic_source_keyword_page_view_window'," +
                "  'username' = 'root'," +
                "  'password' = 'aaaaaa', " +
                "  'sink.properties.format' = 'json', " +
                "  'sink.buffer-count' = '4', " +
                "  'sink.buffer-size' = '4086'," +
                "  'sink.enable-2pc' = 'false', " + // 测试阶段可以关闭两阶段提交,方便测试
                "  'sink.properties.read_json_by_line' = 'true' " +
                ")");
        resTable.executeInsert("dws_traffic_source_keyword_page_view_window");

    }
}
