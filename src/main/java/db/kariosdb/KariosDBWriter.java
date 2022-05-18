package db.kariosdb;


import com.influxdb.client.InfluxDBClientFactory;
import db.Main;
import db.entity.DataPoint;
import db.writer.BaseWriter;
import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.Metric;
import org.kairosdb.client.builder.MetricBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class KariosDBWriter extends BaseWriter {
    private static Logger logger = LoggerFactory.getLogger(KariosDBWriter.class);

    private static HttpClient httpClient ;
    private static String url;
    static {
        url = System.getProperty("kairos_url");
    }

    public KariosDBWriter() {

        createClient();

    }

    public void createClient() {
        try {
            httpClient = new HttpClient("http://localhost:8080");
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
    }



    @Override
    public void saveDataPoint(List<DataPoint> dataPointList) {
        try{
            MetricBuilder builder = convertDataPointToMetric(dataPointList);
            httpClient.pushMetrics(builder);
        }catch (Exception e){
            logger.error("write data error",e);
        }


    }

    private MetricBuilder convertDataPointToMetric(List<DataPoint> dataPointList){
        MetricBuilder builder = MetricBuilder.getInstance();
        DataPoint dataPoint = dataPointList.get(0);
        Metric mt = builder.addMetric(dataPoint.getSensorId());
        List<String> tags = dataPoint.getTags();
        if(tags!=null && tags.size() >0){
            for(String tag: tags){
                mt.addTag(tag,tag);
            }
        }
        for(DataPoint dataPoint1: dataPointList){
            mt.addDataPoint(dataPoint1.getTime(),dataPoint1.getValue());
        }

        return builder ;
    }
}
