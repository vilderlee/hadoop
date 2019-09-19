package com.vilderlee.hadoop.check;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 类说明:
 *
 * <pre>
 * Modify Information:
 * Author        Date          Description
 * ============ ============= ============================
 * VilderLee    2019/9/19      Create this file
 * </pre>
 */
public class CheckMapper extends Mapper<LongWritable, Text, Text, Data> {

    @Override protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // 接收到的每一行数据
        String line = value.toString();
        System.out.println(line);
        JSONObject jsonObject = JSON.parseObject(line);
        String systemNo = (String) jsonObject.get("SystemNo");
        JSONObject data  = (JSONObject) jsonObject.get("Data");
        System.out.println(data);
        context.write(new Text(systemNo), new Data(data.toString()));
    }
}
