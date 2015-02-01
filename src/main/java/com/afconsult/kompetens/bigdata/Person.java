package com.afconsult.kompetens.bigdata;

import org.apache.crunch.Pair;

import java.util.Set;

/**
 * Created by sosandstrom on 2015-02-01.
 */
public class Person {
    private Integer id;
    private String first, last;
    private Set<Person> friends;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getFirst() {
        return first;
    }

    public void setFirst(String first) {
        this.first = first;
    }

    public String getLast() {
        return last;
    }

    public void setLast(String last) {
        this.last = last;
    }

    public Set<Person> getFriends() {
        return friends;
    }

    public void setFriends(Set<Person> friends) {
        this.friends = friends;
    }

    public static final Pair<String, Integer>[] FIRSTS = new Pair[]{
            Pair.of("Maria", 444543),
            Pair.of("Anna", 301945),
            Pair.of("Margareta", 242223),
            Pair.of("Elisabeth", 204463),
            Pair.of("Eva", 191100),
            Pair.of("Birgitta", 172012),
            Pair.of("Kristina", 171597),
            Pair.of("Karin", 159348),
            Pair.of("Elisabet", 142016),
            Pair.of("Marie", 126312),
            Pair.of("Ingrid", 118344),
            Pair.of("Christina", 104844),
            Pair.of("Linnéa", 93296),
            Pair.of("Sofia", 89971),
            Pair.of("Marianne", 88617),
            Pair.of("Kerstin", 88601),
            Pair.of("Lena", 83079),
            Pair.of("Helena", 82407),
            Pair.of("Inger", 74261),
            Pair.of("Emma", 71980),
            Pair.of("Johanna", 71949),
            Pair.of("Linnea", 70201),
            Pair.of("Cecilia", 64310),
            Pair.of("Sara", 63430),
            Pair.of("Elin", 59885),
            Pair.of("Anita", 59473),
            Pair.of("Ulla", 59061),
            Pair.of("Viola", 54408),
            Pair.of("Gunilla", 54158),
            Pair.of("Louise", 53502),
            Pair.of("Erik",300974),
            Pair.of("Lars",233533),
            Pair.of("Karl",206761),
            Pair.of("Anders",191226),
            Pair.of("Johan",173505),
            Pair.of("Per",167402),
            Pair.of("Nils",138347),
            Pair.of("Carl",130308),
            Pair.of("Jan",129219),
            Pair.of("Mikael",128174),
            Pair.of("Lennart",124509),
            Pair.of("Hans",120952),
            Pair.of("Olof",115988),
            Pair.of("Peter",112943),
            Pair.of("Gunnar",112162),
            Pair.of("Sven",109228),
            Pair.of("Bengt",96410),
            Pair.of("Fredrik",95731),
            Pair.of("Bo",89319),
            Pair.of("Åke",85449),
            Pair.of("Daniel",85006),
            Pair.of("Göran",84309),
            Pair.of("Gustav",81818),
            Pair.of("Alexander",74980),
            Pair.of("Magnus",73954),
            Pair.of("Martin",72964),
            Pair.of("Stefan",72341),
            Pair.of("Andreas",71808),
            Pair.of("John",67423),
            Pair.of("Leif",67100)
    };
    public static final Pair<String,Integer>[] LASTS = new Pair[] {
            Pair.of("Johansson",247587),
            Pair.of("Andersson",247169),
            Pair.of("Karlsson",187423),
            Pair.of("Nilsson",168548),
            Pair.of("Eriksson",135145),
            Pair.of("Larsson",122668),
            Pair.of("Olsson",106566),
            Pair.of("Persson",105052),
            Pair.of("Svensson",99663),
            Pair.of("Gustafsson",70469),
            Pair.of("Pettersson",63371),
            Pair.of("Jonsson",56313),
            Pair.of("Jansson",48286),
            Pair.of("Hansson",42756),
            Pair.of("Bengtsson",33471),
            Pair.of("Jönsson",31692),
            Pair.of("Carlsson",29977),
            Pair.of("Petersson",29213),
            Pair.of("Lindberg",27382),
            Pair.of("Magnusson",26289),
            Pair.of("Lindström",25143),
            Pair.of("Gustavsson",24857),
            Pair.of("Olofsson",24341),
            Pair.of("Lindgren",22959),
            Pair.of("Axelsson",22290),
            Pair.of("Bergström",21248),
            Pair.of("Lundberg",21188),
            Pair.of("Lundgren",20601),
            Pair.of("Jakobsson",20562),
            Pair.of("Berg",19966),
            Pair.of("Berglund",19261),
            Pair.of("Fredriksson",17887),
            Pair.of("Mattsson",17804),
            Pair.of("Sandberg",17792),
            Pair.of("Henriksson",16741),
            Pair.of("Sjöberg",16452),
            Pair.of("Forsberg",16393),
            Pair.of("Lindqvist",15919),
            Pair.of("Lind",15668),
            Pair.of("Engström",15492),
            Pair.of("Håkansson",15215),
            Pair.of("Danielsson",15213),
            Pair.of("Eklund",15161),
            Pair.of("Lundin",15114),
            Pair.of("Gunnarsson",14359),
            Pair.of("Holm",14336),
            Pair.of("Bergman",14039),
            Pair.of("Samuelsson",13996),
            Pair.of("Fransson",13827),
            Pair.of("Johnsson",13552),
            Pair.of("Lundqvist",13272),
            Pair.of("Nyström",13269),
            Pair.of("Holmberg",13209),
            Pair.of("Björk",12989),
            Pair.of("Arvidsson",12941),
            Pair.of("Isaksson",12664),
            Pair.of("Wallin",12656),
            Pair.of("Söderberg",12620),
            Pair.of("Nyberg",12606),
            Pair.of("Mårtensson",12313)
    };
}
