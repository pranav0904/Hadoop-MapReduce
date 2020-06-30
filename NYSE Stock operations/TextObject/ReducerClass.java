import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerClass extends Reducer<Text, Text, Text, Text>{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        int maxVolume=Integer.MIN_VALUE;
        int minVolume=Integer.MAX_VALUE;
        String maxDate="", minDate="";
        double maxAdjClosePrice=0.0 ;


        for (Text t: values) {
            //date + "," + volume + ","+AdjPrice;
            String line = t.toString();
            String[] tokens = line.split(",");

            String date=tokens[0];
            int volume= Integer.parseInt(tokens[1]);
            double AdjPrice= Double.parseDouble(tokens[2]);

            if(volume > maxVolume){
                maxVolume = volume;
                maxDate = date;
            }
            if(volume < minVolume){
                minVolume = volume;
                minDate = date;
            }
            if(AdjPrice > maxAdjClosePrice){
                maxAdjClosePrice = AdjPrice;
            }

        }
        String value = "Max AdjClose_Price:  "+maxAdjClosePrice+"\t Max volume:  "+maxVolume+"\ton  "+maxDate+"\t Min volume:  " +minVolume+"\ton  "+minDate;

        context.write(key, new Text(value));

    }
}


/*
    public void reduce(Text key,Iterable<CompositeValueWritable> values,Context context) throws IOException, InterruptedException{
        int maxVolume=Integer.MIN_VALUE;
        int minVolume=Integer.MAX_VALUE;
        String maxDate="", minDate="";
        double maxAdjClosePrice=0.0 ;

        for(CompositeValueWritable s:values){
            if(s.getVolume() > maxVolume){
                maxVolume = s.getVolume();
                maxDate = s.getDate();

            }
            if(s.getVolume() < minVolume){
                minVolume = s.getVolume();
                minDate = s.getDate();
            }
            if(s.getStockprice() > maxAdjClosePrice){
                maxAdjClosePrice = s.getStockprice();
            }
        }

 */