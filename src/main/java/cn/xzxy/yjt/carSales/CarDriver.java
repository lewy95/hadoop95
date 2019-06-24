package cn.xzxy.yjt.carSales;

import cn.xzxy.yjt.profitPartitioner.Profit;
import cn.xzxy.yjt.profitPartitioner.ProfitMapper;
import cn.xzxy.yjt.profitPartitioner.ProfitPartitioner;
import cn.xzxy.yjt.profitPartitioner.ProfitReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CarDriver {

    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InterruptedException {
        //1. 代表hadoop环境的对象
        Configuration conf = new Configuration();

        //2.1创建job对象
        Job job = Job.getInstance(conf);
        //2.2设置运行的主类
        job.setJarByClass(CarDriver.class);

        //3.1设置Mapper组件
        job.setMapperClass(CarMapper.class);
        //3.2设置Mapper输出key的类型
        job.setMapOutputKeyClass(Text.class);
        //3.3设置Mapper输出value的类型
        job.setMapOutputValueClass(Car.class);

        //4.1设置Reducer组件
        job.setReducerClass(CarReduce.class);
        //4.2设置Reducer输出key的类型
        job.setOutputKeyClass(Text.class);
        //4.3设置Reducer输出value的类型
        job.setOutputValueClass(Car.class);

        //5.设置Combiner组件
        job.setCombinerClass(CarCombiner.class);

        //6.设置自定义分区，按照地区进行分区
        //如果不设置，hadoop有一个默认的实现类，HashPartitioner会按照默认方法分区
        job.setPartitionerClass(CarPartitioner.class);
        //如果不设置，还是只有一个分区
        job.setNumReduceTasks(3);

        //7.1设置处理文件的hdfs的输入路径(导包导长的)，路径写到目录级别
        FileInputFormat.setInputPaths(job,
                new Path("hdfs://192.168.80.128:9000/mr/car"));
        //7.2设置结果输出路径,输出目录不能存在
        FileOutputFormat.setOutputPath(job,
                new Path("hdfs://192.168.80.128:9000/mr/car/result"));

        //8.提交job
        job.waitForCompletion(true);
    }
}
