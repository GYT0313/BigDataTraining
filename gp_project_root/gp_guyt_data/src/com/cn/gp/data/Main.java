package com.cn.gp.data;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;
import java.util.UUID;

public class Main {

    public static void main(String[] args) throws InterruptedException {

        if (args.length < 3) {
            System.err.println("Usage: data <source> <nums> <sleep>");
            System.exit(1);
        }

        int nums = Integer.parseInt(args[1]);
        int sleepTime = Integer.parseInt(args[2]);

        String line = "\n";
        while (true) {
            StringBuilder stringBuilder = new StringBuilder();
            for (int i = 0; i < nums; i++) {
                Random random = new Random(System.currentTimeMillis());
                Thread.sleep(random.nextInt(sleepTime) * 500);
                String data = run();
                stringBuilder.append(data + line);
                System.out.println(data);
            }
            writeFile(args[0], stringBuilder);
            System.out.println("----------");
        }
    }

    private static String run() {
        String imei = RandomIM.getRandomIMEI();
        String imsi = RandomIM.getRandomIMSI();

        String longitude = RandomTude.getRandomLongitude();
        String latitude = RandomTude.getRandomLatitude();

        String phoneMac = RandomMAC.getRandomMac();
        String deviceMac = RandomMAC.getRandomMac();

        String deviceNumber = RandomNumber.getRandomDeviceNumber();
        String phone = RandomNumber.getRandomPhoneNumber();

        String collectTime = String.valueOf(System.currentTimeMillis() / 1000);

        String userName = RandomName.getRandomName();

        Random random = new Random(System.currentTimeMillis());
        int index = random.nextInt(Fields.SEARCH_ENGINE.length - 1);
        String engine = RandomSearch.getRandomEngine(index);

        String content = RandomContent.getRandomContent();
        String url = RandomSearch.getRandomSearchURL(index) + content;

        String data = imei + "\t" + imsi + "\t" + longitude + "\t" + latitude + "\t" + phoneMac + "\t" +
                deviceMac + "\t" + deviceNumber + "\t" + collectTime + "\t" + userName + "\t" + phone + "\t" +
                engine + "\t" + url + "\t" + content;

        return data;
    }

    private static void writeFile(String source, StringBuilder data) {
        String name = "search";
        String uuid = UUID.randomUUID().toString();
        String fineName = name + "_" + source + "_" + uuid + ".txt";
        File file = new File(fineName);
        try {
            if (!file.exists()) {
                file.createNewFile();

            }

            FileWriter fileWriter = new FileWriter(file);
            fileWriter.write(data.toString());
            fileWriter.flush();
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
