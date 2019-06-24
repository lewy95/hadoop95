package cn.xzxy.yjt.autoOutput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 自定义输出格式，需要继承FileOutputFormat
 * 用途：数据挖掘时对数据是由需求的
 * @param <K>
 * @param <V>
 */
public class AuthOutputFormat<K,V> extends FileOutputFormat<K,V> {

    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {
        //获取输出的路径
        Path path = super.getDefaultWorkFile(taskAttemptContext,"");

        Configuration conf = taskAttemptContext.getConfiguration();
        FileSystem fileSystem = path.getFileSystem(conf);
        FSDataOutputStream outputStream = fileSystem.create(path);

        //将输出流传给自定义的写出类
        return new AuthRecordWriter(outputStream,"###","@@@");
    }
}
