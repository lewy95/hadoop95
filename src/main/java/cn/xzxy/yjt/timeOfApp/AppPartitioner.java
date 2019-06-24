package cn.xzxy.yjt.timeOfApp;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;



public class AppPartitioner extends Partitioner<Text, App> {


    public int getPartition(Text text, App app, int i) {
        if (app.getType().equals("chat")){
            return 0;
        }
        if (app.getType().equals("game")){
            return 1;
        }
        if (app.getType().equals("shopping")){
            return 2;
        }
        if (app.getType().equals("video")){
            return 3;
        }
        return 0;
    }
}
