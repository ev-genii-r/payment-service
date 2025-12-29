package com.innowise.rudkovskii.service;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public class RandomIntegerGenerator {

    public static int getRandomInteger(){

        int count = 1;
        int min = 1;
        int max = 10;

        String urlString = String.format(
                "https://www.random.org/integers/?num=%d&min=%d&max=%d&col=1&base=10&format=plain&rnd=new",
                count, min, max
        );

        try {
            URL url = new URL(urlString);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");

            int responseCode = connection.getResponseCode();

            if (responseCode != 200) {
                throw new Exception("HTTP error code: " + responseCode);
            }

            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));

            int number = Integer.parseInt(reader.readLine().trim());

            reader.close();
            return number;

        }catch (Exception e){
            e.printStackTrace();
            return 0;
        }

    }
}
