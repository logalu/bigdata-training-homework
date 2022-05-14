package com.exercise;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CellphoneTrafficReducer extends Reducer<Text, CellphoneTraffic, Text, CellphoneTraffic> {

	@Override
	protected void reduce(Text key, Iterable<CellphoneTraffic> values, Context context)
			throws IOException, InterruptedException {


		CellphoneTraffic countedCellphoneTraffic = new CellphoneTraffic();

		for (CellphoneTraffic cellphoneTraffic : values) {
			countedCellphoneTraffic.setPhone(cellphoneTraffic.getPhone());
			countedCellphoneTraffic.setUploadTraffic(countedCellphoneTraffic.getUploadTraffic()+cellphoneTraffic.getUploadTraffic());
			countedCellphoneTraffic.setDownloadTraffic(countedCellphoneTraffic.getDownloadTraffic()+cellphoneTraffic.getDownloadTraffic());
		}

		countedCellphoneTraffic.setTotalTraffic(countedCellphoneTraffic.getUploadTraffic() + countedCellphoneTraffic.getDownloadTraffic());

		context.write(key, countedCellphoneTraffic);
	}

}
