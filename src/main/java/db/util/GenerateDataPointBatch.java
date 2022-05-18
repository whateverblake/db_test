package db.util;

import db.entity.DataPoint;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class GenerateDataPointBatch {

    private static Random random = new Random();

    private static Double generateValue() {
        return random.nextDouble() * 100;
    }


    public static List<DataPoint> generateDataPoint(int batch, String id, long startTime, List<String> tags) {
        List<DataPoint> dataPointList = new ArrayList<>();

        for (int i = 0; i < batch; i++) {
            DataPoint dataPoint = new DataPoint(id, startTime, generateValue(), tags);
            dataPointList.add(dataPoint);
            startTime++;
        }

        return dataPointList;
    }
}
