package cn.xzxy.yjt.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {

    /**
     * 工作原理：
     * 在分布式计算中，MapReduce框架负责处理了并行编程里分布式存储、工作调度，负载均衡、容错处理
     * 以及网络通信等复杂问题，现在我们把处理过程高度抽象为Map与Reduce两个部分来进行阐述，
     * 其中Map部分负责把任务分解成多个子任务，
     * Reduce部分负责把分解后多个子任务的处理结果汇总起来
     * 一个mapreduce程序至少要有三个类：mapper  reduce  driver(连接mapper和reduce)
     */
    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InterruptedException {
        //代表hadoop环境的对象
        Configuration conf = new Configuration();
        //创建job对象
        Job job = Job.getInstance(conf);
        //设置运行的主类
        job.setJarByClass(WordCountDriver.class);

        //设置Mapper组件
        job.setMapperClass(WordCountMapper.class);
        //设置Mapper输出key的类型
        job.setMapOutputKeyClass(Text.class);
        //设置Mapper输出value的类型
        job.setMapOutputValueClass(IntWritable.class);

        //设置Reducer组件
        job.setReducerClass(WordCountReducer.class);
        //设置Reducer输出key的类型
        job.setOutputKeyClass(Text.class);
        //设置Reducer输出value的类型
        job.setOutputValueClass(IntWritable.class);

        //设置处理文件的hdfs的输入路径(导包导长的)，路径写到目录级别
        FileInputFormat.setInputPaths(job,
                new Path("hdfs://192.168.80.128:9000/mr/wordcount"));
        //设置结果输出路径,输出目录不能存在
        FileOutputFormat.setOutputPath(job,
                new Path("hdfs://192.168.80.128:9000/mr/wordcount/output"));

        //提交job
        job.waitForCompletion(true);
    }
}
