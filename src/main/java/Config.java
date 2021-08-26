import java.io.*;
import java.util.*;

class Config {

    public Map<String, String> getValues() throws IOException {

        /* public static void main(String[] args) { */

        Map<String, String> result = new HashMap<>();
        try {
            File file = new File("src/main/resources/config.properties");
            FileInputStream fileInput = new FileInputStream(file);
            Properties properties = new Properties();
            properties.load(fileInput);
            fileInput.close();

            Enumeration enuKeys = properties.keys();
            while (enuKeys.hasMoreElements()) {

                String key = (String) enuKeys.nextElement();
                String value = properties.getProperty(key);
                result.put(key, value);

            }
            System.out.println(result);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }
}

