package com.vilderlee.hadoop.check;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

public class CheckApplication {

    private static final Logger logger = LoggerFactory.getLogger(CheckApplication.class);

    public static void main(String[] args) throws Exception {
        logger.info("start...");
        hadoop();
        logger.info("end...");

    }

    private static void hadoop() throws IOException, InterruptedException, ClassNotFoundException {
        //通过Job来封装本次mr的相关信息
        Configuration conf = new Configuration();
        // 即使没有下面这行,也可以本地运行 因\hadoop-mapreduce-client-core-2.7.4.jar!\mapred-default.xml 中默认的参数就是 local
        //        conf.set("mapreduce.framework.name","local");
        conf.set("param", "first param");
        Job job = Job.getInstance(conf, "job");

        //指定本次mr job jar包运行主类
        job.setJarByClass(CheckApplication.class);

        //指定本次mr 所用的mapper reducer类分别是什么
        job.setMapperClass(CheckMapper.class);
        job.setReducerClass(CheckReducer.class);

        //指定本次mr mapper阶段的输出  k  v类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Data.class);
        //指定本次mr 最终输出的 k v类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // job.setNumReduceTasks(3); //ReduceTask个数

        // 如果业务有需求，就可以设置combiner组件
        // job.setCombinerClass(WordCountReducer.class);

        // 指定本次mr 输入的数据路径 和最终输出结果存放在什么位置
        Random random = new Random();

        local(job, random);
        remote(conf, job, random);

        // MultipleOutputs.addNamedOutput(job, "check", TextOutputFormat.class, Text.class, Text.class);
        // job.submit(); //一般不要这个.
        //提交程序  并且监控打印程序执行情况
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }

    private static void remote(Configuration conf, Job job, Random random) throws IOException {
        Path inputPath = new Path("/check");
        Path outputPath = new Path("/check/output" + random.nextInt());

        FileSystem fileSystem = FileSystem.newInstance(conf);
        if (!fileSystem.exists(inputPath)) {
            fileSystem.mkdirs(inputPath);
            fileSystem.copyFromLocalFile(new Path("F:\\check\\input"), inputPath);
        }

        // 上面的路径是本地测试时使用，如果要打包jar到hdfs上运行时，需要使用下面的路径。
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
    }

    private static void local(Job job, Random random) throws IOException {
        Path inputPath = new Path("F:\\check\\input");
        Path outputPath = new Path("F:\\check\\output" + random.nextInt());
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
    }

}
