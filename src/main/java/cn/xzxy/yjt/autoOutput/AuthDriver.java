package cn.xzxy.yjt.autoOutput;

import cn.xzxy.yjt.wordcount.WordCountMapper;
import cn.xzxy.yjt.wordcount.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class AuthDriver {

    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InterruptedException {
        //代表hadoop环境的对象
        Configuration conf = new Configuration();
        //创建job对象
        Job job = Job.getInstance(conf);
        //设置运行的主类
        job.setJarByClass(AuthDriver.class);

        //设置Mapper组件
        job.setMapperClass(AuthMapper.class);
        //设置Mapper输出key的类型
        job.setMapOutputKeyClass(Text.class);
        //设置Mapper输出value的类型
        job.setMapOutputValueClass(IntWritable.class);

        //设置Reducer组件
        job.setReducerClass(AuthReducer.class);
        //设置Reducer输出key的类型
        job.setOutputKeyClass(Text.class);
        //设置Reducer输出value的类型
        job.setOutputValueClass(IntWritable.class);

        //设置格式输出器
        job.setOutputFormatClass(AuthOutputFormat.class);

        //设置处理文件的hdfs的输入路径(导包导长的)，路径写到目录级别
        FileInputFormat.setInputPaths(job,
                new Path("hdfs://192.168.80.128:9000/mr/word"));
        //设置结果输出路径,输出目录不能存在
        FileOutputFormat.setOutputPath(job,
                new Path("hdfs://192.168.80.128:9000/mr/word/result"));

        //提交job
        job.waitForCompletion(true);
    }
}
