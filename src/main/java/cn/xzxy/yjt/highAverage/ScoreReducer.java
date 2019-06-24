package cn.xzxy.yjt.highAverage;

import cn.xzxy.yjt.fruitTotal.Fruit;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * reduce的组件 继承hadoop.mapreduce.Reducer
 * 参数一、二：输入键类型，输入值类型，同mapper的输出键值类型保持一致
 * 参数三、四：输出键类型，输出值类型
 */
public class ScoreReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        //将Score类放入List,通过ArrayList实现
        List<Score> scores = new ArrayList<Score>();
        //遍历传入的人名和成绩
        for (Text value:values){
            String[] lines = value.toString().split(",");
            Score score = new Score();
            score.setStudentName(lines[0]);//写入人名
            score.setAverage(new Double(lines[1]));//写入平均成绩
            scores.add(score);
        }
        //Collections.sort实现对列表的升序排序
        Collections.sort(scores);
        //Collections.reverse反转升序后的元素,即降序
        Collections.reverse(scores);
        //输出平均成绩最高的前3条记录
        for (int k = 0; k < 3; k++){
            Score s = scores.get(k);
            context.write(key, new Text(s.getStudentName() + "的成绩:" + s.getAverage()));
        }
    }
}
