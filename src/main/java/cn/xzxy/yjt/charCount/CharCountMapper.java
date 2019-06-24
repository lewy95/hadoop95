package cn.xzxy.yjt.charCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Mapper组件，继承hadoop.mapreduce.Mapper
 * 参数1：KEYIN，固定的，代表每行行首偏移量  LongWritable相当于java的long
 * 参数2：VALUEIN，固定的，代表输入的每一行的数据  Text相当于java的String
 * 参数3：KEYOUT，不固定的，代表输出key的数据类型
 * 参数3：VALUEOUT，不固定的，代表输出value的数据类型  IntWritable相当于java的int
 * 步骤一：分割
 * 如文件中为 hello world
 *           bye world
 *           则生成<0,"hello world"><12,"bye world">数字为偏移量，包括空格回车符
 * 步骤二：map()方法
 *       整理成输出格式<hell0,1><world,2><bye,1>
 */
public class CharCountMapper extends Mapper<LongWritable, Text, Text, Text> {
    /* Mapper组件通过map方法将输入key和输入value传给开发者
     * 利用context对象中的write()方法输出key\value
     */
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        //将value转换成java的字符串
        String line = value.toString();
        //截取@和第一个.之间的部分，存入数组
        String[] words = line.split(" ");
        String firstChar;
        for (String word: words) {
            firstChar = word.substring(0,1).toLowerCase();
            context.write(new Text(firstChar), new Text(word));
        }
    }
}
