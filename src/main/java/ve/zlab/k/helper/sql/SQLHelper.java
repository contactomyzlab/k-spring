package ve.zlab.k.helper.sql;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class SQLHelper {
    
    public static String timestampToISO8601(final String property, final String alias) {
        return SQLHelper.timestampToChar(property, "YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"", alias);
    }
    
    public static String dateToISO8601(final String property, final String alias) {
        return SQLHelper.dateToChar(property, "YYYY-MM-DD", alias);
    }
    
    public static String timestampToChar(final String property, final String format, final String alias) {
        return String.format("TO_CHAR(%s AT TIME ZONE 'UTC', '%s') AS %s", property, format, alias);
    }
    
    public static String dateToChar(final String property, final String format, final String alias) {
        return String.format("TO_CHAR(%s, '%s') AS %s", property, format, alias);
    }
    
    public static List<Double> stringToListDouble(final String c) {
        if (c == null) {
            return null;
        }
        
        final String[] tokens = c.trim().split(",");
        
        if (tokens.length == 0) {
            return null;
        }
        
        final List<Double> list = new ArrayList<>();
        
        for (final String token : tokens) {
            try {
                list.add(Double.parseDouble(token.trim()));
            } catch (Exception e) {
                //NOTHING TO DO
            }
        }
        
        if (list.isEmpty()) {
            return null;
        }
        
        return list;
    }
    
    public static List<Integer> stringToListInteger(final String c) {
        if (c == null) {
            return null;
        }
        
        final String[] tokens = c.trim().split(",");
        
        if (tokens.length == 0) {
            return null;
        }
        
        final List<Integer> list = new ArrayList<>();
        
        for (final String token : tokens) {
            try {
                list.add(Integer.parseInt(token.trim()));
            } catch (Exception e) {
                //NOTHING TO DO
            }
        }
        
        if (list.isEmpty()) {
            return null;
        }
        
        return list;
    }
    
    public static List<Long> stringToListLong(final String c) {
        if (c == null) {
            return null;
        }
        
        final String[] tokens = c.trim().split(",");
        
        if (tokens.length == 0) {
            return null;
        }
        
        final List<Long> list = new ArrayList<>();
        
        for (final String token : tokens) {
            try {
                list.add(Long.parseLong(token.trim()));
            } catch (Exception e) {
                //NOTHING TO DO
            }
        }
        
        if (list.isEmpty()) {
            return null;
        }
        
        return list;
    }
    
    public static List<String> stringToListString(final String c) {
        if (c == null) {
            return null;
        }
        
        final String[] tokens = c.trim().split(",");
        
        if (tokens.length == 0) {
            return null;
        }
        
        final List<String> list = new ArrayList<>();
        
        for (final String token : tokens) {
            try {
                list.add(token);
            } catch (Exception e) {
                //NOTHING TO DO
            }
        }
        
        if (list.isEmpty()) {
            return null;
        }
        
        return list;
    }
    
    public static List<UUID> stringToListUUID(final String c) {
        if (c == null) {
            return null;
        }
        
        final String[] tokens = c.trim().split(",");
        
        if (tokens.length == 0) {
            return null;
        }
        
        final List<UUID> list = new ArrayList<>();
        
        for (final String token : tokens) {
            try {
                list.add(UUID.fromString(token));
            } catch (Exception e) {
                //NOTHING TO DO
            }
        }
        
        if (list.isEmpty()) {
            return null;
        }
        
        return list;
    }
    
    public static List<Boolean> stringToListBoolean(final String c) {
        if (c == null) {
            return null;
        }
        
        final String[] tokens = c.trim().split(",");
        
        if (tokens.length == 0) {
            return null;
        }
        
        final List<Boolean> list = new ArrayList<>();
        
        for (final String token : tokens) {
            try {
                list.add(Boolean.parseBoolean(token));
            } catch (Exception e) {
                //NOTHING TO DO
            }
        }
        
        if (list.isEmpty()) {
            return null;
        }
        
        return list;
    }
    
    public static List<UUID> listStringToListUUID(final List<String> c) {
        if (c == null) {
            return null;
        }
        
        final List<UUID> list = new ArrayList<>();
        
        if (c.isEmpty()) {
            return list;
        }

        for (final String token : c) {
            try {
                list.add(UUID.fromString(token));
            } catch (Exception e) {
                //NOTHING TO DO
            }
        }
        
        if (list.isEmpty()) {
            return null;
        }
        
        return list;
    }
    
    public static Set<String> stringToSetString(final String c) {
        if (c == null) {
            return null;
        }
        
        final String[] tokens = c.trim().split(",");
        
        if (tokens.length == 0) {
            return null;
        }
        
        final Set<String> list = new HashSet<>();
        
        for (final String token : tokens) {
            try {
                list.add(token);
            } catch (Exception e) {
                //NOTHING TO DO
            }
        }
        
        if (list.isEmpty()) {
            return null;
        }
        
        return list;
    }
}
