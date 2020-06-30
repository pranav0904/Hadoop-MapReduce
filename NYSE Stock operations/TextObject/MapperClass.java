import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;



public class MapperClass extends Mapper<LongWritable, Text,Text, Text> {


    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] tokens = line.split(",");

        String StockSymbol = tokens[1];

        String date = tokens[2];
        int volume = Integer.parseInt(tokens[7]);
        double AdjPrice = Double.valueOf(tokens[8]);

        String val = date + "," + volume + ","+AdjPrice;

        context.write(new Text(StockSymbol),new Text(val));

    }
}