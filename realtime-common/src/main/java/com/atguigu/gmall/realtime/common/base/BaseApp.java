package com.atguigu.gmall.realtime.common.base;

import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * FlinkAPI应用程序的基类
 * 模板方法设计模式，在父类中定义完成某一个功能的核心算法骨架（步骤），有些步骤在父类中没有办法实现，需要延迟到子类中去完成
 *      好处：约定了模板
 *          在不改变弗雷核心算法骨架的前提下，每个子类都可以有自己不同的实现
 */

public abstract class BaseApp {
    //入口执行
    public void start(int port, int parallelism, String ckAndGroupId, String topic) throws Exception {
        //todo 1.基本环境准备
        //1.1 指定流处理环境,并指定本地测试时启动 WebUI 所绑定的端口
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        //1.2 设置并行度
        env.setParallelism(parallelism);
        //todo 2.检查点相关的设置
        //2.1 开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE); //5秒，精准一次性
        //2.2 设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000L); //一分钟
        //2.3 设置job取消后检查点是否保留
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4 设置两个检查点之间最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L); //如果说默认只能有一个检查点备份（可以设置多个），第1个检查点6s才备份完，那第二个检查点按理说5s开启，但现在只能8s的时候才能开启，因为最小间隔2s
        //2.5 重启策略，默认的是Integer的最大值
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L)); //重启3次，3s尝试一次
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3))); //30天只能重启三次
        //2.6 设置状态后端以及检查点存储路径
//        env.setStateBackend(new HashMapStateBackend()); //状态存在tm的堆内存里，检查点存在Jm的堆内存里
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/ck/" + ckAndGroupId);
        //2.7 设置操作hadoop的用户
        System.setProperty("HADOOP_USER_NAME","atguigu");

        //todo 3.从kafka的topic_db主题中读取业务数据
        //3.1 声明消费的主题以及消费者组
        //3.2 创建消费者对象
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(topic, ckAndGroupId);

        //3.3 消费数据 封装为流
        DataStreamSource<String> kafkaStrDs = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka_Source");

        //todo 4.处理逻辑
        //模板方法设计方式
        hadle(env,kafkaStrDs);
        //todo 5.提交代码
        env.execute();

    }

    //定义一个抽象方法，只能提供方法的声明，但具体实现我没有
    public abstract void hadle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDs);
}
