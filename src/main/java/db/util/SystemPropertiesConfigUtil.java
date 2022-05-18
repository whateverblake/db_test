package db.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SystemPropertiesConfigUtil {

    private static Logger logger = LoggerFactory.getLogger(SystemPropertiesConfigUtil.class);


    public static String getSystemConfigFileDirectory(String propertyName, String baseDirectory) {
        String directory = System.getProperty(propertyName);
        if (directory == null) {
            directory = System.getProperty("user.dir") +"/"+ baseDirectory+"/";
            logger.info("no {} is specified by user so use the default = {} ", propertyName, directory);
        }
        return directory;
    }
}
