import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import static java.lang.Float.parseFloat;

public class MapperClass extends Mapper<LongWritable, Text, Text, FloatWritable> {
    Text stock_symbol = new Text();
    FloatWritable high_price = new FloatWritable();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String [] Tokens = line.split(",");
        //TOKENS[1]:SYMBOL
        //TOKENs[4]:HIGH_PRICE
        // For rest of data it goes here
        if (Float.parseFloat(Tokens[4]) > 0)
        {
            stock_symbol.set(Tokens[1]);
            high_price.set(Float.parseFloat(Tokens[4]));
            context.write(stock_symbol,high_price);
        }

    }
}