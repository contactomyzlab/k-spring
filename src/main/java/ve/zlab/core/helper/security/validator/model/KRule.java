package ve.zlab.core.helper.security.validator.model;

public class KRule {
    public static int LONG_MAX = 1;
    public static int STRING_NOT_NULL_NOT_EMPTY = 2;
    public static int STRING_PARSEABLE_TO_DOUBLE = 3;
    public static int LONG_IN = 4;
    public static int OBJECT_NOT_NULL = 5;
    public static int INTEGER_BETWEEN = 6;
    public static int LONG_NOT_NULL_NOT_ZERO = 7;
    public static int STRING_MAX = 8;
    public static int SQL_EXISTS_BY_COLUMN = 9;
    public static int BOOLEAN_FALSE = 10;
    public static int OBJECT_NULL = 11;
    public static int LONG_BETWEEN = 12;
    public static int EMAIL_PATTERN = 13;
    public static int SQL_NOT_EXISTS_BY_COLUMN = 14;
    public static int INTEGER_GREATER_THAN = 15;
    public static int LIST_NOT_EMPTY = 16;
    public static int SQL_NOT_EXISTS_BY_COLUMN_EXCLUDING_BY_ID = 17;
    public static int INTEGER_GREATER_THAN_OR_EQUAL_TO = 18;
    public static int DATE_ISO8601 = 19;
    public static int LIST_MIN = 20;
    public static int LIST_PROPERTY_STRING_PARSEABLE_TO_DOUBLE = 21;
    public static int LIST_PROPERTY_STRING_DOUBLE_BETWEEN = 22;
    public static int LIST_MIN_OR_EMPTY = 23;
    public static int DOUBLE_NOT_NULL = 24;
    public static int DOUBLE_GREATER_THAN = 25;
    public static int LIST_ALL_EXISTS_ON = 26;
    public static int STRING_MIN = 27;
    public static int STRING_ONLY_LETTERS = 28;
    public static int STRING_LETTERS_WHITESPACES = 29;
    public static int SQL_NOT_IEXISTS_BY_COLUMN = 30;
    public static int STRING_LETTERS_WHITESPACES_ACENTS = 31;
    public static int STRING_ONLY_NUMBERS = 32;
    public static int STRING_ALPHANUMERIC = 33;
    public static int INTEGER_IN = 34;
    public static int STRING_IN = 35;
    public static int DOUBLE_GREATER_THAN_OR_EQUAL_TO = 36;
    public static int LIST_PROPERTY_OBJECT_NOT_NULL = 37;
    public static int STRING_ONLY_LOWERCASE_LETTERS = 38;
    public static int STRING_ONLY_UPPERCASE_LETTERS = 39;
    public static int STRING_LOWERCASE_LETTERS_UNDERSCORE = 40;
    public static int STRING_UPPERCASE_LETTERS_UNDERSCORE = 41;
    
    protected String message;
    private int rule;

    public KRule(final String message, final int rule) {
        this.message = message;
        this.rule = rule;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public int getRule() {
        return rule;
    }

    public void setRule(int rule) {
        this.rule = rule;
    }
    
    
}
