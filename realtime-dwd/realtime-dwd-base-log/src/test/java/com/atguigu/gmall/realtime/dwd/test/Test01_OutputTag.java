package com.atguigu.gmall.realtime.dwd.test;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.OutputTag;

/**
 * 该案例演示了测输出流标签创建问题
 */
public class Test01_OutputTag {
    public static void main(String[] args) {
        OutputTag<String> dirtyTag = new OutputTag<>("dirtyTag", TypeInformation.of(String.class));
    }
}
