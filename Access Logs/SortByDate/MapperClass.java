import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperClass extends Mapper<LongWritable, Text, CompositeKeyWritable, IntWritable> {

    IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //127.0.0.1 - - [15/Oct/2011:11:49:11 -0400] "GET / HTTP/1.1" 200 44

        String line = value.toString();
        String[] tokens = line.split(" ");

        String webIp = null;
        String timestamp = null;
        String val[] = null;

        timestamp = tokens[3].replace("[","");
        String[] dates = timestamp.split(":");
        val=dates[0].split("/");
        String date=null;

        try {
            if (tokens.length == 10) {
                webIp = tokens[0];
                date = val[2] +"/"+ val[1]+"/" + val[0];

            } else{
                webIp = "Unknown";
                date = "N/A";
            }
        } catch (Exception ex) {
            //Do Nothing
        }

        CompositeKeyWritable obj = new CompositeKeyWritable(webIp, date);

        context.write(obj, one);
    }
}