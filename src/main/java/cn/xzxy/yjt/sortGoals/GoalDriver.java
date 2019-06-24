package cn.xzxy.yjt.sortGoals;

import cn.xzxy.yjt.fruitTotal.Fruit;
import cn.xzxy.yjt.fruitTotal.FruitMapper;
import cn.xzxy.yjt.fruitTotal.FruitReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class GoalDriver {

    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InterruptedException {
        //1. 代表hadoop环境的对象
        Configuration conf = new Configuration();

        //2.1创建job对象
        Job job = Job.getInstance(conf);
        //2.2设置运行的主类
        job.setJarByClass(GoalDriver.class);

        //3.1设置Mapper组件
        job.setMapperClass(GoalsMapper.class);
        //3.2设置Mapper输出key的类型
        job.setMapOutputKeyClass(IntWritable.class);
        //3.3设置Mapper输出value的类型
        job.setMapOutputValueClass(Goal.class);

        //4.1设置Reducer组件
        job.setReducerClass(GoalReducer.class);
        //4.2设置Reducer输出key的类型
        job.setOutputKeyClass(IntWritable.class);
        //4.3设置Reducer输出value的类型
        job.setOutputValueClass(Goal.class);

        //5.1设置处理文件的hdfs的输入路径(导包导长的)，路径写到目录级别
        FileInputFormat.setInputPaths(job,
                new Path("hdfs://192.168.80.128:9000/mr/goals"));
        //5.2设置结果输出路径,输出目录不能存在
        FileOutputFormat.setOutputPath(job,
                new Path("hdfs://192.168.80.128:9000/mr/goals/result"));

        //6.提交job
        job.waitForCompletion(true);
    }
}
