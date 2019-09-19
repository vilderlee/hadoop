package com.vilderlee.hadoop.check;

import com.alibaba.fastjson.JSON;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

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
public class CheckReducer extends Reducer<Text, Data, Text, Text> {

    private static final Logger logger = LoggerFactory.getLogger(CheckReducer.class);

    @Override protected void reduce(Text key, Iterable<Data> values, Context context)
            throws IOException, InterruptedException {

        String param = context.getConfiguration().get("param");
        logger.info("param:" + param);
        List<HashMap> list = new ArrayList<>();

        for (Data data : values) {
            logger.info(data.toString());
            HashMap jsonObject = JSON.parseObject(data.getData(), HashMap.class);
            list.add(jsonObject);
        }
        logger.info("up: start" + list);
        List<HashMap> up = null;
        try {
            up = list.stream().filter(map -> {
                return map.get("stream").equals("up");
            }).collect(Collectors.toList());
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            logger.info("up:" + up);
        }
        try {
            MultipleOutputs<Text, Text> mos = new MultipleOutputs<>(context);
            up.forEach(map -> {
                try {
                    logger.info(JSON.toJSON(map).toString());
                    context.write(new Text(""), new Text(JSON.toJSON(map).toString()));
//                    mos.write("check", new Text(""), new Text(JSON.toJSON(map).toString()), "error");
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
