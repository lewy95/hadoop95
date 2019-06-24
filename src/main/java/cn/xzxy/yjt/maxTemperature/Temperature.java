package cn.xzxy.yjt.maxTemperature;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Temperature implements Writable{

    private String city;
    private String date;
    private int temp;

    @Override
    public String toString() {
        return "Temperature{" +
                "city='" + city + '\'' +
                ", date='" + date + '\'' +
                ", temp=" + temp +
                '}';
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public int getTemp() {
        return temp;
    }

    public void setTemp(int temp) {
        this.temp = temp;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(city);
        dataOutput.writeUTF(date);
        dataOutput.writeInt(temp);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.city = dataInput.readUTF();
        this.date = dataInput.readUTF();
        this.temp = dataInput.readInt();
    }
}
