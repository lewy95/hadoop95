package cn.xzxy.yjt.invertedIndex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        //System.out.println("------reduce");

        //生成文档列表
        StringBuffer fileList = new StringBuffer();
        String fileIndex;
        String wordIndex;
        for (Text value : values) {
            String[] tokens = value.toString().split(":");
            fileIndex = tokens[0];
            wordIndex = tokens[1];
            fileList = fileList.append("(" + fileIndex + "," + wordIndex + ")");
        }
        result.set(fileList.toString());

        //System.out.println(key + "," + result);
        context.write(key, result);
    }
}
