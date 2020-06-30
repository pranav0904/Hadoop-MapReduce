import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeKeyWritable implements WritableComparable<CompositeKeyWritable> {

    String webIp;
    String date;

    public CompositeKeyWritable(){
    }

    public CompositeKeyWritable(String webIp, String date) {
        this.webIp = webIp;
        this.date = date;
    }

    public String getWebIp() {
        return webIp;
    }

    public void setWebIp(String webIp) {
        this.webIp = webIp;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public void readFields(DataInput in) throws IOException {
        webIp = in.readUTF();
        date = in.readUTF();
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(webIp);
        out.writeUTF(date);
    }

    public int compareTo(CompositeKeyWritable o) {
        int result = this.date.compareTo(o.getDate());
        return (result < 0 ? -1 : (result == 0 ? 0 : 1));
    }

    @Override
    public String toString() {
        return webIp + " \t " + date;
    }
}