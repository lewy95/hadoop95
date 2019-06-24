package cn.xzxy.yjt.autoOutput;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class AuthRecordWriter<K, V> extends RecordWriter<K, V> {

    private FSDataOutputStream outputStream;
    private String kvSpilt;
    private String lineSpilt;


    public AuthRecordWriter(FSDataOutputStream outputStream, String kvSpilt, String lineSpilt) {
        this.outputStream = outputStream;
        this.kvSpilt = kvSpilt;
        this.lineSpilt = lineSpilt;
    }

    public void write(K k, V v) throws IOException, InterruptedException {
        outputStream.write(k.toString().getBytes());
        outputStream.write(kvSpilt.getBytes());
        outputStream.write(v.toString().getBytes());
        outputStream.write(lineSpilt.getBytes());
    }

    public void close(TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {
        outputStream = null;
    }
}
