package cn.xzxy.yjt.joinTable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TableDriver {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(TableDriver.class);

        job.setMapperClass(TableMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Table.class);

        job.setReducerClass(TableReducer.class);
        job.setOutputKeyClass(Table.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.80.128:9000/mr/ordertable"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.80.128:9000/mr/ordertable/result"));

        job.waitForCompletion(true);
    }
}
