package cn.xzxy.yjt.autopay;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PayDriver {

    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InterruptedException {
        //1. 代表hadoop环境的对象
        Configuration conf = new Configuration();

        //2.1创建job对象
        Job job = Job.getInstance(conf);
        //2.2设置运行的主类
        job.setJarByClass(PayDriver.class);

        //3.1设置Mapper组件
        job.setMapperClass(PayMapper.class);
        //3.2设置Mapper输出key的类型
        job.setMapOutputKeyClass(Text.class);
        //3.3设置Mapper输出value的类型
        job.setMapOutputValueClass(Text.class);

        //4.设置自定义格式读取器
        //如果不设置，hadoop会默认调用自带的格式处理器TextInputFormate
        job.setInputFormatClass(PayInputFormat.class);

        //5.1设置处理文件的hdfs的输入路径(导包导长的)，路径写到目录级别
        FileInputFormat.setInputPaths(job,
                new Path("hdfs://192.168.80.128:9000/mr/autopay"));
        //5.2设置结果输出路径,输出目录不能存在
        FileOutputFormat.setOutputPath(job,
                new Path("hdfs://192.168.80.128:9000/mr/autopay/result"));

        //6.提交job
        job.waitForCompletion(true);
    }
}
