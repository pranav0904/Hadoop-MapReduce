import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;


public class CompositeValueWritable implements Writable,WritableComparable<CompositeValueWritable> {
    private String stockName;
    private String date;
    private int volume;
    private double stockprice;

    private String maxDate;
    private String minDate;
    private int maxVolume;
    private int minVolume;

    private double maxPrice;

    public CompositeValueWritable(String stockName, String date, int volume, double stockprice) {
        this.date = date;
        this.volume = volume;
        this.stockName = stockName;
        this.stockprice = stockprice;
    }

    public CompositeValueWritable() {

    }

    public int compareTo(CompositeValueWritable s) {
        int result = stockName.compareTo(s.stockName);
        if (result == 0) {
            result = date.compareTo(s.date);
        }
        return result;
    }

    public void setStockName(String stockName) {
        this.stockName = stockName;
    }

    public String getDate() {
        return date;
    }

    public int getVolume() {
        return volume;
    }

    public double getStockprice() {
        return stockprice;
    }

    public void write(DataOutput d) throws IOException {
        WritableUtils.writeString(d, stockName);
        WritableUtils.writeString(d, date);
        WritableUtils.writeVInt(d, volume);
        WritableUtils.writeString(d, String.valueOf(stockprice));

    }

    public void readFields(DataInput in) throws IOException {
        stockName = WritableUtils.readString(in);
        date = WritableUtils.readString(in);
        volume = WritableUtils.readVInt(in);
        stockprice = Double.valueOf(WritableUtils.readString(in));
    }

    public void setMaxDate(String maxDate) {
        this.maxDate = maxDate;
    }

    public void setMinDate(String minDate) {
        this.minDate = minDate;
    }

    public void setMaxPrice(double maxPrice) {
        this.maxPrice = maxPrice;
    }

    public void setMaxVolume(int maxVolume) {
        this.maxVolume = maxVolume;
    }

    public void setMinVolume(int minVolume) {
        this.minVolume = minVolume;
    }

    public String toString() {
        return new StringBuilder().append("\t Max Volume :").append(maxVolume).append("\t On ").append(maxDate).append("\t").
                append("\t Min Volume :  ").append(minVolume).append("\t On ").append(minDate).append("\t Max Adj_close : ").append(maxPrice).toString();
    }
}
