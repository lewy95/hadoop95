package cn.xzxy.yjt.autopay;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.io.InputStream;

/**
 * 此类称为格式读取器，用于决定如何读取文件数据
 * 1.hadoop有一个默认的格式读取器，对应的类交LineRecordReader
 *   此读取器会默认将行首偏移量作为key，每行数据作为value
 * 2.initialize()此方法是组件的初始化方法，只会调用一次，主要是初始化切片对象，文件系统对象等
 * 3.nextKeyValue()此方法会被调用多次，如果return true，则会继续调用，如果return false则停止调用
 *   此方法作用：此方法通过LineReader处理文件内容，初始化输入key和输入value
 * 4.getCurrentKey() \ getCurrentValue()作用是将key和value传给mapper
 *   nextKeyValue()方法每被调用一次，getCurrentKey() \ getCurrentValue()也会被跟着调用一次
 *
 */
public class PayRecordReader extends RecordReader<Text, Text> {

    //将初始化变量定义为类的成员变量
    private FileSplit fileSplit;
    private LineReader reader;
    //定义自定义输入的key
    private Text key;
    private Text value;

    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        //初始化切片对象
        fileSplit = (FileSplit) inputSplit;
        Path path = fileSplit.getPath();
        //获取环境对象
        Configuration conf = taskAttemptContext.getConfiguration();
        //获取文件系统对象
        FileSystem fileSystem = path.getFileSystem(conf);
        //获取处理文件的输入流
        InputStream inputStream = fileSystem.open(path);
        //初始化行读取器，行读取器是hadoop提供的工具类，可以一行一行处理内容
        reader = new LineReader(inputStream);
    }

    public boolean nextKeyValue() throws IOException, InterruptedException {
        key = new Text();
        value = new Text();
        Text tmp = new Text();
        int length = reader.readLine(tmp);
        if (length == 0) {
            //长度=0，表示没有数据可读，可以终止此方法了
            return false;
        } else {
            String fileName = fileSplit.getPath().getName().split("\\.")[0];
            key.set(fileName);

            String str = tmp.toString();
            reader.readLine(tmp);
            str = str + "#" + tmp;
            reader.readLine(tmp);
            str = str + "#" + tmp;
            value.set(str);
            return true;
        }
    }

    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }


    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    public void close() throws IOException {
        reader = null;
    }
}
