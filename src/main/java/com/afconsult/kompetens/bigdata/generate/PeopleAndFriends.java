package com.afconsult.kompetens.bigdata.generate;

import com.afconsult.kompetens.bigdata.Friendship;
import com.afconsult.kompetens.bigdata.Person;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import org.apache.crunch.Pair;

import java.io.*;
import java.util.HashMap;

/**
 * Created by sosandstrom on 2015-02-01.
 */
public class PeopleAndFriends {

    public static void main(String[] args) throws IOException {
        ImmutableMap.Builder<Integer, Person> peopleBuilder = ImmutableMap.<Integer, Person>builder();

        int sum = 0;
        for (Pair<String, Integer> first : Person.FIRSTS) {
            sum += first.second();
        }
        final int SUM_FIRSTS = sum;
        sum = 0;
        for (Pair<String, Integer> last : Person.LASTS) {
            sum += last.second();
        }
        final int SUM_LASTS = sum;

        final File srcMainResources = new File("src/main/resources");

        final File people = new File(srcMainResources, "people.csv");
        final PrintWriter peopleWriter = new PrintWriter(new FileWriter(people));
        final HashMap<Integer, Person> PEOPLE = new HashMap<Integer, Person>();
        for (int i = 1; i <= 10000; i++) {
            Person p = new Person();
            p.setId(i);
            long firstRand = Math.round(Math.random()*SUM_FIRSTS);
            long lastRand = Math.round(Math.random()*SUM_LASTS);
            p.setFirst(getName(firstRand, Person.FIRSTS));
            p.setLast(getName(lastRand, Person.LASTS));
            PEOPLE.put(i, p);

            peopleWriter.print(Integer.toString(p.getId()));
            peopleWriter.print(',');
            peopleWriter.print(p.getFirst());
            peopleWriter.print(',');
            peopleWriter.println(p.getLast());
        }
        peopleWriter.close();

        // let's make friends:
        final File friendsFile = new File(srcMainResources, "friends.csv");
        final PrintWriter friendWriter = new PrintWriter(new FileWriter(friendsFile));
        int id = 1;
        for (Person one : PEOPLE.values()) {
            for (Person other : PEOPLE.values()) {
                if (one.getId() != other.getId() && (
                        one.getFirst().equals(other.getFirst()) ||
                        one.getLast().equals(other.getLast())
                        )) {
                    friendWriter.println(String.format("%d,%d,%d", id++, one.getId(), other.getId()));
                }
            }
        }
        friendWriter.close();
    }

    private static String getName(long rand, Pair<String, Integer>[] names) {
        long sum = 0;
        for (Pair<String, Integer> pair : names) {
            sum += pair.second();
            if (rand <= sum) {
                return pair.first();
            }
        }
        throw new IllegalArgumentException("rand is larger than frequency sum");
    }

    public static final String FIRST_NAMES[] = {"Anna", "Bertil", "Cecilia", "David", "Emelie", "Fredrik",
            "Gunnar", "Harriet", "Ingela", "Johan", "Kristina", "Linnea", "Mikael", "Nora",
            "Olof", "Patricia", "Qvintus", "Renee", "Stig", "Tuva", "Urban", "Viola", "Xerxes",
            "Yvette", "Zlatan"
    };

    public static final String LAST_NAMES[] = {"Andersson", "Bildt", "Curie", "Darwin", "Ericsson", "Fermat",
            "Golding", "Holm", "Ibrahimovic", "Jobs", "Kant", "Lewis", "Murdock", "Nobel", "Ohm", "Pele",
            "Quinn", "Rosengren", "Spielberg", "Trotskij", "Underwood", "Woods"
    };

}
