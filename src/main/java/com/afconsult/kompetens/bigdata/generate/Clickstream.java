package com.afconsult.kompetens.bigdata.generate;

import com.afconsult.kompetens.bigdata.Person;
import com.google.common.collect.ImmutableMap;
import org.apache.crunch.Pair;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;

/**
 * Created by sosandstrom on 2015-02-01.
 */
public class Clickstream {

    private static final String[] CATEGORY = {"Apparel", "Books", "Cycling", "Diving",
            "Electronics", "Fishing", "Golf", "Hotels",
            "Innovations", "Opera", "Mountains", "Laundry",
            "Music", "Sports", "Travel", "Universe"};

    public static void main(String[] args) throws IOException {

        final File srcMainResources = new File("src/main/resources");

        final File clickstream = new File(srcMainResources, "access.log");
        final PrintWriter clickWriter = new PrintWriter(new FileWriter(clickstream));

        final TreeMap<Integer, Integer> productProbabilities = new TreeMap<Integer, Integer>();
        final TreeMap<Integer, Integer> productCategories = new TreeMap<Integer, Integer>();

        // generate product maps
        int productId = 1000;
        long totalProbability = 0l;

        for (int cat = 0; cat < 16; cat++) {
            for (int i = 0; i < 16; i++) {
                final int probability = Math.round((float)(Math.random()*1000));
                totalProbability += probability;
                final int id = productId++;

                productProbabilities.put(id, probability);
                productCategories.put(id, cat);
            }
        }

        // generate click stream
        long timestamp = 1423397141L;
        for (int userId = 1000000; userId < 1100000; userId++) {
            int clickCount = (int) Math.round(Math.ceil(Math.log10(1+(Math.random()*10000))));
            final ArrayList<Integer> clickedCategories = new ArrayList<Integer>();

            for (int c = 0; c < clickCount; c++) {
                Integer pid = getCorrelatedProduct(clickedCategories, totalProbability, productProbabilities, productCategories);
                int cat = productCategories.get(pid);

                // write to clickstream
                // IP,userId,timestamp,GET /pdp_PID.html,status,latency
                String s = String.format("127.0.0.1\t%d\t%d\tGET /pdp_%s_%d.html\t200\t152",
                        userId, timestamp++, CATEGORY[cat], pid);
                clickWriter.println(s);

                clickedCategories.add(cat);
            }
        }

        clickWriter.close();
    }

    private static Integer getCorrelatedProduct(ArrayList<Integer> clickedCategories, long totalProbability,
                                                TreeMap<Integer, Integer> productProbabilities, TreeMap<Integer, Integer> productCategories) {
        // boost each clicked category product by 100;
        long increasedProbability = totalProbability + 1600*clickedCategories.size();
        long sample = Math.round(Math.random()*increasedProbability);

        int productId = 999;
        long sum = 0;
        do {
            productId++;
            sum += productProbabilities.get(productId);
            int cat = productCategories.get(productId);
            for (Integer clickedCat : clickedCategories) {
                if (cat == clickedCat) {
                    sum += 100;
                }
            }

        } while (sum < sample);
        return productId;
    }

}
