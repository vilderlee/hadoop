package com.vilderlee.hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

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
public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    @Override protected void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {

        long sum = 0;
        for (LongWritable value : values) {
            // 求key出现的次数总和
            sum += value.get();
        }

        // 最终统计结果的输出
        context.write(key, new LongWritable(sum));
    }
}