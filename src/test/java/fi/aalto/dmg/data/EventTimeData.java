package fi.aalto.dmg.data;

import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by jun on 30/11/15.
 */
public class EventTimeData {

    public static List<Tuple3<Long, String, Integer>> personWithAge = Arrays.asList(
            new Tuple3<>(1448892364000L, "Miko", 15), new Tuple3<>(1448892367000L, "Jace", 25),
            new Tuple3<>(1448892368000L, "Rose", 25), new Tuple3<>(1448892372000L, "Hari", 43),
            new Tuple3<>(1448892374000L, "Jack", 35), new Tuple3<>(1448892378000L, "Jason", 37),
            new Tuple3<>(1448892382000L, "Mick", 21), new Tuple3<>(1448892389000L, "Kk", 25),
            new Tuple3<>(1448892390000L, "Duke", 25), new Tuple3<>(1448892399000L, "Duma", 28),
            new Tuple3<>(1448892404000L, "Anna", 22), new Tuple3<>(1448892406000L, "Erik", 30),
            new Tuple3<>(1448892412000L, "Jarn", 35), new Tuple3<>(1448892414000L, "Nick", 37),
            new Tuple3<>(1448892414040L, "Kuke", 23), new Tuple3<>(1448892415100L, "Json", 39),
            new Tuple3<>(1448892417400L, "Deak", 28), new Tuple3<>(1448892423010L, "Jim", 29),
            new Tuple3<>(1448892425110L, "Obama", 55), new Tuple3<>(1448892425810L, "Busi", 65),
            new Tuple3<>(1448892427010L, "Jook", 34), new Tuple3<>(1448892429110L, "Nkin", 25),
            new Tuple3<>(1448892430110L, "Mari", 25), new Tuple3<>(1448892432110L, "Mike", 26),
            new Tuple3<>(1448892436110L, "Uoks", 28), new Tuple3<>(1448892446110L, "Duli", 95),
            new Tuple3<>(1448892504000L, "Mkke", 25), new Tuple3<>(1448892506500L, "Kom", 45),
            new Tuple3<>(1448892508200L, "Niok", 32), new Tuple3<>(1448892509000L, "Jeep", 34),
            new Tuple3<>(1448892512300L, "Ikls", 25), new Tuple3<>(1448892515300L, "Lksa", 12),
            new Tuple3<>(1448892517300L, "LJks", 22), new Tuple3<>(1448892537300L, "Luks", 15)
    );

    // Rose 10s later, Hari 5s later, Anna 3s earlier, Ikls 30s later
    // Make missing
    public static List<Tuple3<Long, String, String>> personWithInterest = Arrays.asList(
            new Tuple3<>(1448892363000L, "Miko", "Apple"), new Tuple3<>(1448892368000L, "Jace", "Game"),
            new Tuple3<>(1448892375000L, "Rose", "Organge"), new Tuple3<>(1448892375000L, "Hari", "Banana"),
            new Tuple3<>(1448892375200L, "Jack", "Chatting"), new Tuple3<>(1448892380000L, "Jason", "Walking"),
            new Tuple3<>(1448892382000L, "Mick", "Egg"), new Tuple3<>(1448892389000L, "Kk", "Cabbage"),
            new Tuple3<>(1448892390400L, "Duke", "Chilli"), new Tuple3<>(1448892401000L, "Duma", "Reading"),
            new Tuple3<>(1448892401000L, "Anna", "Cycling"), new Tuple3<>(1448892406000L, "Erik", "Kayka"),
            new Tuple3<>(1448892412000L, "Jarn", "Singing"), new Tuple3<>(1448892414000L, "Nick", "Reading"),
            new Tuple3<>(1448892414040L, "Kuke", "iPhone"), new Tuple3<>(1448892415100L, "Json", "Mac"),

            new Tuple3<>(1448892415240L, "Kuse", "OnePlus"), new Tuple3<>(1448892415400L, "Jsonn", "iMac"),

            new Tuple3<>(1448892419400L, "Deak", "Stories"), new Tuple3<>(1448892423910L, "Jim", "Pating"),
            new Tuple3<>(1448892425110L, "Obama", "Dawing"), new Tuple3<>(1448892425910L, "Busi", "Coding"),
            new Tuple3<>(1448892427310L, "Jook", "Smiles"), new Tuple3<>(1448892429110L, "Nkin", "Jump"),
            new Tuple3<>(1448892434110L, "Mari", "Shopping"), new Tuple3<>(1448892439110L, "Mike", "Forest"),
            new Tuple3<>(1448892446110L, "Uoks", "Lake"), new Tuple3<>(1448892446310L, "Duli", "Boat"),
            new Tuple3<>(1448892504000L, "Mkke", "Music"), new Tuple3<>(1448892506900L, "Kom", "Food"),
            new Tuple3<>(1448892509200L, "Niok", "Learing"), new Tuple3<>(1448892511000L, "Jeep", "Games"),
            new Tuple3<>(1448892532300L, "Ikls", "Warcraft"), new Tuple3<>(1448892534300L, "Lksa", "Dota"),
            new Tuple3<>(1448892536300L, "LJks", "Age3"), new Tuple3<>(1448892568300L, "Luks", "nothing")
    );

}
