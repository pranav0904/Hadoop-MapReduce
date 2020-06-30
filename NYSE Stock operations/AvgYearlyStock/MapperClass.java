import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperClass extends Mapper<LongWritable, Text,Text, DoubleWritable>{

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        try{
            String[] Tokens = value.toString().split(",");
            String StockSymbol = Tokens[1];
            double price = Double.parseDouble(Tokens[8]);

            String[] date = Tokens[2].split("-");
            String year = date[0];

            String Key = StockSymbol + "  " + year;

            context.write(new Text(Key), new DoubleWritable(price));

        }catch (Exception ae){
            //Do nothing
        }
    }
}
/*
String line = value.toString();
        String[] tokens = line.split(",");
 */
