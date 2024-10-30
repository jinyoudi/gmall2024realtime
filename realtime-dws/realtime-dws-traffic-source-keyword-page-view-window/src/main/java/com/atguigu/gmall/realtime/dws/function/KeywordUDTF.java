package com.atguigu.gmall.realtime.dws.function;

/*
自定义udtf函数
 */

import com.atguigu.gmall.realtime.dws.util.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;


@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeywordUDTF extends TableFunction<Row> {
    public void eval(String text) {
        for (String keyword : KeywordUtil.analyze(text)) {
            // use collect(...) to emit a row
            collect(Row.of(keyword));
        }
    }
}
