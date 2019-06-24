package cn.xzxy.yjt.autoScore;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * 自定义格式输入器：用于自定义mapper的输入key和mapper的输入value
 * 第一个泛型是自定义输入key的泛型
 * 第二个泛型是自定义输入value的泛型
 */
public class ScoreInputFormat extends FileInputFormat<Text,Text> {

    /**
     * 此方法允许要求返回一个RecordReader，此对象可以定义自定义输入的格式
     * 以及如何将自定义输入的key和value传给mapper
     * @param inputSplit
     * @param taskAttemptContext
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public RecordReader<Text, Text>
    createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {
        return new ScoreRecordReader();
    }
}
