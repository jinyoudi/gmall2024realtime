package com.atguigu.gmall.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import com.sun.scenario.effect.impl.sw.sse.SSEBlend_SRC_OUTPeer;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Set;

/**
 * 操作hbase的工具类
 */

public class HBaseUtil {
    //获取hbase连接
    // 官网:https://hbase.apache.org/devapidocs/org/apache/hadoop/hbase/client/ConnectionFactory.html
    public static Connection getHbaseConnection() throws IOException {

        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");
//        conf.set("hbase.zookeeper.property.clientPort", "2181");

        Connection hbaseConn = ConnectionFactory.createConnection(conf);
        return hbaseConn;
    }
    //关闭hbase连接
    public static void closeHbaseConnection(Connection hbaseConn) throws IOException{
        if(hbaseConn != null && !hbaseConn.isClosed()){
            hbaseConn.close();
        }
    }

    //建表
    public static void createHBaseTable (Connection hbaseConn,String namespace,String tableName,String ... families) throws IOException {
        if(families.length < 1){
            System.out.println("至少需要一个列族");
        }
        try(Admin admin = hbaseConn.getAdmin()){
            TableName tableNameObj = TableName.valueOf(namespace, tableName);
            if(admin.tableExists(tableNameObj)){
                System.out.println("表空间" + namespace + "下的表" + tableName + "已存在");
                return;
            }
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableNameObj);
            for (String family : families) {
                ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family)).build();
                tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
            }
            System.out.println("表空间" + namespace + "下的表" + tableName + "创建成功");

            admin.createTable(tableDescriptorBuilder.build());
        }catch (IOException e){
            throw new RuntimeException(e);
        }
    }

    //删除表
    public static void dropHBaseTable(Connection hbaseConn,String nameSpace,String tableName) throws IOException {
        try(Admin admin = hbaseConn.getAdmin()){
            //判断删除的表是否存在
            TableName tableNameObj = TableName.valueOf(nameSpace, tableName);
            if(!admin.tableExists(tableNameObj)){
                System.out.println("要删除的表弓箭"+nameSpace+"下的表"+tableName+"不存在");
                return  ;
            }
            admin.disableTable(tableNameObj); //删除前先disable一下
            admin.deleteTable(tableNameObj);
            System.out.println("删除的表空间"+nameSpace+"下的表"+tableName+"");
        }catch (IOException e){
            throw new RuntimeException(e);
        }
    }

    /**
     * 向表中put数据
     * @param hbaseConn 连接对象
     * @param namespace 表空间
     * @param tableName 表名
     * @param rowKey
     * @param family 列族
     * @param jsonObj 要put的数据
     */
    public static void putRow(Connection hbaseConn, String namespace, String tableName, String rowKey, String family, JSONObject jsonObj){
        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        try (Table table = hbaseConn.getTable(tableNameObj)){
            Put put = new Put(Bytes.toBytes(rowKey));
            Set<String> columns = jsonObj.keySet();
            for (String colum : columns) {
                String value = jsonObj.getString(colum);
                //StringUtils要用org.apache.commons.lang3
                if(StringUtils.isNotEmpty(value)){
                    //开始放入数据
                    put.addColumn(Bytes.toBytes(family),Bytes.toBytes(colum),Bytes.toBytes(value));
                }
            }
            table.put(put);
            System.out.println("向表空间"+namespace+"下的表"+tableName+"中put数据 "+ rowKey +"成功");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //从表中删除数据
    public static void delRow(Connection hbaseConn, String namespace, String tableName, String rowKey){
        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        try (Table table = hbaseConn.getTable(tableNameObj)){
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
            System.out.println("从表空间"+namespace+"下的表"+tableName+"中p删除数据 "+ rowKey +"成功");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
