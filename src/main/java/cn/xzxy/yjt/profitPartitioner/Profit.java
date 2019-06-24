package cn.xzxy.yjt.profitPartitioner;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Profit implements Writable{

    private String month;
    private String name;
    private int income;
    private int tax;

    @Override
    public String toString() {
        return "Profit{" +
                "month='" + month + '\'' +
                ", name='" + name + '\'' +
                ", income=" + income +
                ", tax=" + tax +
                '}';
    }

    public String getMonth() {
        return month;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getIncome() {
        return income;
    }

    public void setIncome(int income) {
        this.income = income;
    }

    public int getTax() {
        return tax;
    }

    public void setTax(int tax) {
        this.tax = tax;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(month);
        dataOutput.writeUTF(name);
        dataOutput.writeInt(income);
        dataOutput.writeInt(tax);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.month = dataInput.readUTF();
        this.name = dataInput.readUTF();
        this.income = dataInput.readInt();
        this.tax = dataInput.readInt();
    }
}
