package cn.xzxy.yjt.invertedIndex;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text keyInfo = new Text();  //存储单词和URI的组合
    private Text valueInfo = new Text();//存储词频

    @Override
    public void map(LongWritable longWritable, Text value, Context context)
            throws IOException, InterruptedException {
        //System.out.println("-----mapper");

        //获得<key,value>对所属的FileSplit对象
        FileSplit split = (FileSplit) context.getInputSplit();
        StringTokenizer itr = new StringTokenizer(value.toString());

        while (itr.hasMoreTokens()) {
            //key值由单词和URI组成，如"MapReduce:1.txt"
            keyInfo.set(itr.nextToken() + ":" + split.getPath().getName().split("\\.")[0]);
            //词频初始为1
            valueInfo.set("1");
            //System.out.println(keyInfo + "," + valueInfo);
            context.write(keyInfo, valueInfo);
        }
    }
}
