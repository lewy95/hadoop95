package cn.xzxy.yjt.scorePartitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ScorePartitioner extends Partitioner<Text, Score> {

    public int getPartition(Text key, Score score, int i) {
        if (key.toString().startsWith("z")){
            return 0;
        }
        if (key.toString().startsWith("l")){
            return 1;
        }
        if (key.toString().startsWith("w")){
            return 2;
        }
        return 0;
    }
}
