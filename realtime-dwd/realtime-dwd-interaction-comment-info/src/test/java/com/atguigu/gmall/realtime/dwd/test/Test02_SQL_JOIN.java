package com.atguigu.gmall.realtime.dwd.test;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * 该案例演示了通过FlinkSQL实现双流join
 *
 *                      左表              右表
 *    内连接       OnCreateAndWrite    OnCreateAndWrite
 *    左外连接      OnReadAndWrite      OnCreateAndWrite
 *    右外连接      OnCreateAndWrite    OnReadAndWrite
 *    全外连接      OnReadAndWrite      OnReadAndWrite
 *
 *    //设置状态失效时间和类型，OnCreateAndWrite创建和写的时候才会失效
 *                         valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10))
 *                                         .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
 *                                 .build());
 */

public class Test02_SQL_JOIN {
    public static void main(String[] args) {
        //todo 1.基本环境准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(1);
        //1.3 指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //1.4 设置状态的保留时间
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10)); //保存10s
        //todo 2.检查点相关的设置（略）
        //todo 3.从指定的网络端口读取员工数据 并转换为动态表
        SingleOutputStreamOperator<Emp> empDS = env
                .socketTextStream("hadoop102", 8888)
                .map(new MapFunction<String, Emp>() {
                    @Override
                    public Emp map(String lineStr) throws Exception {
                        String[] fieldArr = lineStr.split(",");
                        return new Emp(Integer.valueOf(fieldArr[0]), fieldArr[1], Integer.valueOf(fieldArr[2]), Long.valueOf(fieldArr[3]));
                    }
                });
        tableEnv.createTemporaryView("emp",empDS);

        //todo 4.从指定的网络端口读取部门数据 并转换为动态表
        SingleOutputStreamOperator<Dept> deptDS = env
                .socketTextStream("hadoop102", 8889)
                .map(new MapFunction<String, Dept>() {
                    @Override
                    public Dept map(String lineStr) throws Exception {
                        String[] fieldArr = lineStr.split(",");
                        return new Dept(Integer.valueOf(fieldArr[0]), fieldArr[1], Long.valueOf(fieldArr[2]));
                    }
                });
        tableEnv.createTemporaryView("dept",deptDS);

        //todo 5.内连接
        //注意：如果使用普通的内外连接，底层会为参与连接的两张表各自维护一个状态，用于存放两张表的数据，默认情况下，状态永不会失效
        //在生产环境一定要设置状态的保存时间
        //为了测试，只开启一个join昂，不然开俩有一个没法停
//        tableEnv.executeSql("select e.empno,e.ename,d.deptno,d.dname from emp e join dept d on e.deptno = d.deptno").print();

        //todo 6.左外连接
        //注意 如果左表数据线到，右表数据厚道，会产生三条数据
//        | op |       empno |                          ename |      deptno |                          dname |
//        | +I |         100 |                             zs |      <NULL> |                         <NULL> |
//        | -D |         100 |                             zs |      <NULL> |                         <NULL> |
//        | +I |         100 |                             zs |          10 |                          ceshi |
        //这样的动态表转换的流称为回撤流
//        tableEnv.executeSql("select e.empno,e.ename,d.deptno,d.dname from emp e left join dept d on e.deptno = d.deptno").print();

        //todo 7.右外连接
//        tableEnv.executeSql("select e.empno,e.ename,d.deptno,d.dname from emp e right join dept d on e.deptno = d.deptno").print();

        //todo 8.全外连接
//        tableEnv.executeSql("select e.empno,e.ename,d.deptno,d.dname from emp e full join dept d on e.deptno = d.deptno").print();

        //todo 9.将左外连接结果写入到kafka主题
        //注意：kafka连接器不支持写入的时候包含update或者delete，需要upsert kafka
        //如果左外连接，左表数据先到，右表数据后到 会有3条数据产生，这样的数据如果写入kafka主题中，kafka主题会接收到3条消息，第一条是左表有右表为空，第二条是一个null消息，第三条是左表右表都有
        //当从kafka主题中读数据的时候存在空消息，如果使用flinksql读取的方式读取，会自动地将空消息过滤掉，如果使用的是flinkapi的方式读取的话，默认的SimpleStringSchema是处理不了空消息的，需要自定义反序列化器
        //除了空消息外，在dws层进行汇总操作的时候，还需要做去重处理
        //9.1 创建一个动态表和要写入的kafka主题进行映射
        tableEnv.executeSql("CREATE TABLE emp_dept (\n" +
                "empno integer,\n" +
                "ename string,\n" +
                "deptno integer,\n" +
                "dname string,\n" +
                "PRIMARY KEY (empno) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = 'first',\n" +
                "  'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")");
        //9.2 写入
        tableEnv.executeSql("insert into emp_dept select e.empno,e.ename,d.deptno,d.dname from emp e left join dept d on e.deptno = d.deptno");
    }
}
