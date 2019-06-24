package cn.xzxy.yjt.scorePartitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ScoreReducer extends Reducer<Text, Score, Text, Score> {

    @Override
    protected void reduce(Text key, Iterable<Score> values, Context context)
            throws IOException, InterruptedException {
        Score score = new Score();
        for (Score value: values) {
            score.setName(value.getName());
            score.setMath(value.getMath() + score.getMath());
            score.setChinese(value.getChinese() + score.getChinese());
            score.setEnglish(value.getEnglish() + score.getEnglish());
        }
        context.write(key,score);
    }
}
