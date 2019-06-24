package cn.xzxy.yjt.highAverage;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Score implements WritableComparable<Score> {

    private String subjectName;
    private String studentName;
    private Double average;

    public String getSubjectName() {
        return subjectName;
    }

    public void setSubjectName(String subjectName) {
        this.subjectName = subjectName;
    }

    public String getStudentName() {
        return studentName;
    }

    public void setStudentName(String studentName) {
        this.studentName = studentName;
    }

    public Double getAverage() {
        return average;
    }

    public void setAverage(Double average) {
        this.average = average;
    }

    @Override
    public String toString() {
        return "Score{" +
                "subjectName='" + subjectName + '\'' +
                ", studentName='" + studentName + '\'' +
                ", average='" + average + '\'' +
                '}';
    }

    /**
     * 没有不实现Compare接口，mapreduce使用默认方法排序
     * @param o
     * @return
     */
    public int compareTo(Score o) {
        return this.average.compareTo(o.getAverage());
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(studentName);
        dataOutput.writeUTF(subjectName);
        dataOutput.writeDouble(average);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.studentName = dataInput.readUTF();
        this.subjectName = dataInput.readUTF();
        this.average = dataInput.readDouble();
    }
}
