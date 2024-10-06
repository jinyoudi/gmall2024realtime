package com.atguigu.gmall.realtime.dwd.db.app;

//如果api和sql混着使用，最后的env.execute要按照最后是动态表还是api操作选择，如果是动态表，就不需要execute

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdInteractionCommentInfo extends BaseSQLApp {
    public static void main(String[] args) {
//        //todo 1.基本环境准备
//        //1.1 指定流处理环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        //1.2 设置并行度
//        env.setParallelism(4);
//        //1.3 指定表执行环境
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//        //todo 2.检查点相关的设置
//        /*
//        //2.1 开启检查点
//        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        //2.2 设置检查点超时时间
//        env.getCheckpointConfig().setCheckpointTimeout(6000L);
//        //2.3 设置状态取消后，检查点是否保留
//        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        //2.4 设置两个检查点之间最小时间间隔
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
//        //2.5 设置重启策略
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));
//        //2.6 设置状态后端
//        env.setStateBackend(new HashMapStateBackend());
//        //2.7 设置操作hadoop的用户
//        System.setProperty("HADOOP_USER_NAME","atguigu");
//         */

        new DwdInteractionCommentInfo().start(
                10012,
                4,
                Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO
        );
    }

    @Override
    public void hadle(StreamTableEnvironment tableEnv) {
        //todo 3.从kafka的topic_db主题中读取数据 创建动态表
        readOdsDb(tableEnv,Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO); //放到基类里面去了，但我觉得没必要。。。没办法跟着课程走吧
        //kafka连接器
        //todo 4.过滤出评论数据
        //where table='comment_info' type='insert'
        Table commentInfo = tableEnv.sqlQuery("select \n" +
                "  `data`['id'] as id,\n" +
                "  `data`['user_id'] as user_id,\n" +
                "  `data`['sku_id'] as sku_id,\n" +
                "  `data`['appraise'] as appraise,\n" +
                "  `data`['comment_txt'] as comment_txt,\n" +
                "  ts,\n" +
                "  proc_time\n" +
                "from topic_db where `table`='comment_info' and `type`='insert'");
//        commentInfo.execute().print();
        //将表对象注册到表执行环境中
        tableEnv.createTemporaryView("comment_info",commentInfo);

        //todo 5.从hbase中读取字典数据 创建动态表
        //hbase连接器
        readBaseDic(tableEnv);
        //todo 6.将评论表和字典表进行关联
        //lookup join
        Table joinedTable = tableEnv.sqlQuery("SELECT c.id,\n" +
                "    c.user_id,\n" +
                "    c.sku_id,\n" +
                "    c.appraise,\n" +
                "    dic.dic_name as appraise_name,\n" +
                "    c.comment_txt,\n" +
                "    c.ts\n" +
                "FROM comment_info AS c\n" +
                "  JOIN base_dic FOR SYSTEM_TIME AS OF c.proc_time AS dic\n" +
                "    ON c.appraise = dic.dic_code;");
//        joinedTable.execute().print();
        //todo 7.将关联的结果写入kafka主题中
        //upsertkafka连接器
        //7.1 创建动态表和要写入的主题进行映射
        tableEnv.executeSql("CREATE TABLE "+Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO+" (\n" +
                "    id string,\n" +
                "    user_id string,\n" +
                "    sku_id string,\n" +
                "    appraise string,\n" +
                "    appraise_name string,\n" +
                "    comment_txt string,\n" +
                "    ts bigint,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                ") " + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));
        //7.2 写入
        joinedTable.executeInsert(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
        //todo 8.提交作业
    }

}
