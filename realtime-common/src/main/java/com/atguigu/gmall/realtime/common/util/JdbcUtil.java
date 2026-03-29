package com.atguigu.gmall.realtime.common.util;

import com.atguigu.gmall.realtime.common.constant.Constant;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 通过jdbc操作mysql数据库
 */

public class JdbcUtil <T>{
    //获取mysql连接
    public static Connection getMysqlConnection() throws Exception {
        //注册驱动
        Class.forName("com.mysql.cj.jdbc.Driver");
        //建立连接
        java.sql.Connection conn = DriverManager.getConnection(Constant.MYSQL_URL, Constant.MYSQL_USER_NAME, Constant.MYSQL_PASSWORD);
        return conn;
    }

    //关闭mysql连接
    public static void closeMySQLConnection(Connection conn) throws SQLException {
        if(conn != null && !conn.isClosed()){
            conn.close();
        }
    }


    //需要从数据库表中查询数据
    //泛型模板,如果是静态方法的话，泛型写在返回值前面,把T的类型告诉：Class<T> clz
    public static <T> List<T> querList(Connection conn,String sql,Class<T> clz,boolean... isUnderlineToCamel) throws Exception {
        //结果
        List<T> resList = new ArrayList<>();

        boolean defaultIsUToC = false;  // 默认不执行下划线转驼峰

        if (isUnderlineToCamel.length > 0) {
            defaultIsUToC = isUnderlineToCamel[0];
        }

        PreparedStatement ps = conn.prepareStatement(sql); //执行声明
        ResultSet rs = ps.executeQuery(); //执行sql返回结果
        ResultSetMetaData metaData = rs.getMetaData(); //获取元数据信息
        while(rs.next()){
            //通过反射创建一个对象，用于接受查询结果
            T obj = clz.newInstance();
            for (int i = 1; i <= metaData.getColumnCount(); i++) { //根据元数据信息遍历每一列
                String columnName = metaData.getColumnName(i);
                Object columnValue = rs.getObject(i);
                //给对象的属性赋值,
                //判断
                if(defaultIsUToC){//如果执行转换成驼峰
                    //把下划线转换成小驼峰
                    columnName = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,columnName);
                }
                //第一个参数：给哪个对象赋值。第二个参数：属性的名字。第三个参数：属性的值
                BeanUtils.setProperty(obj,columnName,columnValue);
            }
            resList.add(obj);
        }
        return resList;
    }
}
