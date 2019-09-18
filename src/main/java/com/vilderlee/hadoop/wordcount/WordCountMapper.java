package com.vilderlee.hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.stream.Stream;

/**
 * 类说明:
 *
 * <pre>
 * Modify Information:
 * Author        Date          Description
 * ============ ============= ============================
 * VilderLee    2019/9/10      Create this file
 * </pre>
 */

/**
 *
 * KEYIN, 输入Key的键值类型
 * VALUEIN, Value类型
 * KEYOUT, Key数据键值类型
 * VALUEOUT Value类型
 *
 *
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    LongWritable one = new LongWritable(1);

    @Override protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // 接收到的每一行数据
        String line = value.toString();

        //按照指定分隔符进行拆分
        String[] words = line.split(" ");

        for (String word : words) {
            // 通过上下文把map的处理结果输出
            context.write(new Text(word), one);
        }

    }
}
