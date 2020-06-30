import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerClass extends Reducer<Text, CompositeValueWritable, Text,  CompositeValueWritable>{

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
        CompositeValueWritable stock=new CompositeValueWritable();

        stock.setStockName(key.toString());
        stock.setMaxPrice(maxAdjClosePrice);
        stock.setMaxDate(maxDate);
        stock.setMinDate(minDate);
        stock.setMaxVolume(maxVolume);
        stock.setMinVolume(minVolume);

        context.write(key, stock);

    }
}
