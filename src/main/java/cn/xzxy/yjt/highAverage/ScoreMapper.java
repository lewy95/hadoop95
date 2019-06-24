package cn.xzxy.yjt.highAverage;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.DecimalFormat;

public class ScoreMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        //输入类型为<偏移量,一行文本>,输出类型为<Text,Text>
        String[] lines=value.toString().split(",");
        Score score = new Score();
        //第一个字符串为课程,写入输出key
        score.setSubjectName(lines[0]);
        score.setStudentName(lines[1]);
        //按下标得到[2]以后所有字符串,转换为double求和
        double sum=0;
        for (int i=2; i<lines.length; i++){
            sum += new Double(lines[i]);
        }
        //DecimalFormat规定小数点后保留两位
        DecimalFormat df=new DecimalFormat("0.00");
        //输出value为人名,平均成绩
        context.write(new Text(score.getSubjectName()),
                new Text(score.getStudentName() + ","
                        + df.format (sum/lines.length-2)));
    }
}
