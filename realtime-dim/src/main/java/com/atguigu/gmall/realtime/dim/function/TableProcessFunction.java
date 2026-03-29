package com.atguigu.gmall.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.JdbcUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.*;
import java.util.*;

/**
 * 处理主流业务和广播流配置数据之后的关联数据
 */

//第一个参数：第一条流的类型。第二个参数，第二条流的类型。第三个参数，输出的类型是什么(二元组，第一个是影响的那行数据，第二个是这条维度数据的配置信息)
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject,TableProcessDim>>{

    private MapStateDescriptor<String, TableProcessDim> mapStateDescriptor;

    //第一个参数表名 第二个参数配置流
    private Map<String,TableProcessDim> configMap = new HashMap<>();

    public TableProcessFunction(MapStateDescriptor<String, TableProcessDim> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //将配置表中的配置信息预加载到configMap中

//        //注册驱动
//        Class.forName("com.mysql.cj.jdbc.Driver");
//        //建立连接
//        java.sql.Connection conn = DriverManager.getConnection(Constant.MYSQL_URL, Constant.MYSQL_USER_NAME, Constant.MYSQL_PASSWORD);
//        //获取数据库操作对象
//        String sql = "select * from gmall2024_config.table_process_dim";
//        PreparedStatement ps = conn.prepareStatement(sql);
//        //执行sql语句
//        ResultSet rs = ps.executeQuery();
//        ResultSetMetaData metaData = rs.getMetaData(); //获取元数据信息
//        //处理结果集
//        while(rs.next()){
//            JSONObject jsonObj = new JSONObject();
//            //jdbc是个例外，从1开始
//            for (int i = 1; i <= metaData.getColumnCount() ; i++) {
//                String columnName = metaData.getColumnName(i);
//                Object columnValue = rs.getObject(i);
//                jsonObj.put(columnName,columnValue);
//            }
//            //将jsonObj转换为实体类对象，并放到configMap中
//            TableProcessDim tableProcessDim = jsonObj.toJavaObject(TableProcessDim.class);
//            configMap.put(tableProcessDim.getSourceTable(),tableProcessDim);
//        }
//
//        //释放资源
//        rs.close();
//        ps.close();
//        conn.close();

        /**
         * jdbc封装成工具类之后
         */
        Connection mysqlConnection = JdbcUtil.getMysqlConnection();
        List<TableProcessDim> tableProcessDimList = JdbcUtil.querList(mysqlConnection, "select * from gmall2024_config.table_process_dim", TableProcessDim.class, true);
        for (TableProcessDim tableProcessDim : tableProcessDimList) {
            configMap.put(tableProcessDim.getSourceTable(),tableProcessDim);
        }
        JdbcUtil.closeMySQLConnection(mysqlConnection);
    }

    //processElement处理主流业务数据 根据维度表名到广播状态中读取配置信息，判断是否为维度
    @Override
    public void processElement(JSONObject jsonObj, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject,TableProcessDim>>.ReadOnlyContext ctx, Collector<Tuple2<JSONObject,TableProcessDim>> collector) throws Exception {
        //获取处理的数据的表名
        String table = jsonObj.getString("table");
        //获取广播状态
        ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        //根据表名，先到广播状态中获取对应的配置信息，如果没有找到对应的配置，再尝试到configmap中获取
        TableProcessDim tableProcessDim = null;
        //先走的广播状态，后走的configmap
        if((tableProcessDim = broadcastState.get(table)) != null
                || (tableProcessDim = configMap.get(table)) != null ){
            //如果根据表名获取到了对应的配置信息，说明当前处理的是维度数据，将维度数据继续向下游传递

            //将维度数据继续向下游传递（只需传递data属性内容即可）
            JSONObject dataJsonObj = jsonObj.getJSONObject("data");

            //在向下游传递数据之前，过滤掉不需要传递的属性
            String sinkColumns = tableProcessDim.getSinkColumns();
            //定义了一个过滤方法
            deleteNotNeedColumns(dataJsonObj,sinkColumns);

            //在向下游传递数据钱，补充对应维度数据的操作类型属性
            String type = jsonObj.getString("type");
            dataJsonObj.put("type",type);

            collector.collect(Tuple2.of(dataJsonObj,tableProcessDim));
        }
    }

    //processBroadcastElement处理广播流配置信息 将配置数据放到广播状态中或从广播状态中删除对应的配置 k：维度表名 v：一个配置对象
    @Override
    public void processBroadcastElement(TableProcessDim tp, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject,TableProcessDim>>.Context ctx, Collector<Tuple2<JSONObject,TableProcessDim>> collector) throws Exception {
        //获取对配置表进行的操作的类型
        String op = tp.getOp();
        //获取广播状态
        BroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        //获取维度表名称
        String sourceTable = tp.getSourceTable();
        if("d".equals(op)){
            //从配置表中删除了一条数据，将对应的配置信息也从广播状态中删除
            broadcastState.remove(sourceTable);
            //configMap里面也得删掉，
            configMap.remove(sourceTable);
        }else{
            //可能对配置表进行读取 添加 或者更新操作，需要将最新配置信息放到广播状态中
            broadcastState.put(sourceTable,tp);
            configMap.put(sourceTable,tp); //这里可以不加，加不加无所谓
        }

    }

    //过滤掉不需要传递的字段
    //dataJsonObj {"tm_name":"Redmi","create_time":"2021-12-14 00:00:00","logo_url":"122","id":1}
    //sinkColumns id,tm_name
    private static void deleteNotNeedColumns(JSONObject dataJsonObj, String sinkColumns) {
        List<String> columnList = Arrays.asList(sinkColumns.split(","));

        Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();

        //如果sinkColumns里面不包含dataJsonObj里面的key，那dataJsonObj删除这个key
        entrySet.removeIf(entry-> !columnList.contains(entry.getKey()));
    }
}
