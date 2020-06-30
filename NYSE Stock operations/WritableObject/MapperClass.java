import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;



public class MapperClass extends Mapper<LongWritable, Text,Text, CompositeValueWritable> {


    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String stockInfo = value.toString();
        String[] s = stockInfo.split(",");
        String name = s[1];
        String date = s[2];
        try {
            int volume = Integer.parseInt(s[7]);
            double price = Double.valueOf(s[8]);
            CompositeValueWritable stock = new CompositeValueWritable(name, date, volume, price);
            context.write(new Text(name), stock);

        } catch (Exception e) {

        }

    }
}