package cn.xzxy.yjt.differEmail;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class DifferEmailDriver {

    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InterruptedException {
        //1. 代表hadoop环境的对象
        Configuration conf = new Configuration();
//        conf.set("fs.default.name", "hdfs://192.168.80.128:9000");
//        conf.set("mapreduce.app-submission.cross-platform", "true");
//        conf.set("mapred.jar","D:\\mavenWorkSet\\hadoop95\\out\\artifacts\\hadoop95_jar\\hadoop95.jar");
//        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
//        if (otherArgs.length < 2) {
//            System.err.println("Usage: wordcount <in> [<in>...] <out>");
//            System.exit(2);
//        }

        //2.1创建job对象
        Job job = Job.getInstance(conf);
        //2.2设置运行的主类
        job.setJarByClass(DifferEmailDriver.class);

        //3.1设置Mapper组件
        job.setMapperClass(DifferEmailMapper.class);
        //3.2设置Mapper输出key的类型
        job.setMapOutputKeyClass(Text.class);
        //3.3设置Mapper输出value的类型
        job.setMapOutputValueClass(IntWritable.class);

        //4.1设置Reducer组件
        job.setReducerClass(DifferEmailReducer.class);
        //4.2设置Reducer输出key的类型
        job.setOutputKeyClass(Text.class);
        //4.3设置Reducer输出value的类型
        job.setOutputValueClass(IntWritable.class);

        //5.1设置处理文件的hdfs的输入路径(导包导长的)，路径写到目录级别
        FileInputFormat.setInputPaths(job,new Path("hdfs://192.168.80.128:9000/mr/email"));
        //5.2设置结果输出路径,输出目录不能存在
        FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.80.128:9000/mr/email/result"));

//        for (int i = 0; i < otherArgs.length - 1; ++i) {
//            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
//        }
//        FileOutputFormat.setOutputPath(job,
//                new Path(otherArgs[otherArgs.length - 1]));

        //6.提交job
        job.waitForCompletion(true);
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
