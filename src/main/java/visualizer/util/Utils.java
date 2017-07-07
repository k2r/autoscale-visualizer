/**
 * 
 */
package visualizer.util;

import java.util.ArrayList;

/**
 * @author Roland
 *
 */
public class Utils {

	public static <T extends Number> T getMinValue(ArrayList<T> values){
		T min = values.get(0);
		int size = values.size();
		for(int i = 0; i < size; i++){
			T current = values.get(i);
			if(current.doubleValue() < min.doubleValue()){
				min = current;
			}
		}
		return min;
	}
	
	public static <T extends Number> T getMaxValue(ArrayList<T> values){
		T max = values.get(0);
		int size = values.size();
		for(int i = 1; i < size; i++){
			T current = values.get(i);
			if(current.doubleValue() > max.doubleValue()){
				max = current;
			}
		}
		return max;
	}
	
	public static <T extends Number> Double getAvgValue(ArrayList<T> values){
		Double sum = 0.0;
		Double count = 0.0;
		int size = values.size();
		for(int i = 0; i < size; i++){
			sum += values.get(i).doubleValue();
			count++;
		}
		return sum / count;
	}
}
