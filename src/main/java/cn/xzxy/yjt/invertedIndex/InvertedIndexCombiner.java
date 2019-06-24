package cn.xzxy.yjt.invertedIndex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text> {
    private Text info = new Text();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        //System.out.println("------combiner");
        //统计词频
        int sum = 0;
        for (Text value : values) {
            sum += Integer.parseInt(value.toString());
        }
        int splitIndex = key.toString().indexOf(":");

        //重新设置value值由URI和词频组成
        info.set(key.toString().substring(splitIndex + 1) + ":" + sum);
        //重新设置key值为单词
        key.set(key.toString().substring(0, splitIndex));
        //System.out.println(key + "," +info);
        context.write(key, info);
    }
}
