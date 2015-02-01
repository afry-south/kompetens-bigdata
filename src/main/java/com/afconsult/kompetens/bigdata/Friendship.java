package com.afconsult.kompetens.bigdata;

import org.apache.crunch.Tuple3;

/**
 * Created by sosandstrom on 2015-02-01.
 */
public class Friendship extends Tuple3<Person, Integer, Person> {
    public Friendship(Person one, Integer id, Person other) {
        super(one, id, other);
    }

    public Person one() {
        return first();
    }

    public Integer id() {
        return second();
    }

    public Person other() {
        return third();
    }
}
