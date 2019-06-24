package cn.xzxy.yjt.carSales;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Car implements Writable {

    private String type;
    private String name;
    private int sale;
    private int month;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getSale() {
        return sale;
    }

    public void setSale(int sale) {
        this.sale = sale;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    @Override
    public String toString() {
        return "Car{" +
                "type='" + type + '\'' +
                ", name='" + name + '\'' +
                ", sale=" + sale +
                ", month=" + month +
                '}';
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(type);
        dataOutput.writeUTF(name);
        dataOutput.writeInt(sale);
        dataOutput.writeInt(month);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.type = dataInput.readUTF();
        this.name = dataInput.readUTF();
        this.sale = dataInput.readInt();
        this.month = dataInput.readInt();
    }
}
