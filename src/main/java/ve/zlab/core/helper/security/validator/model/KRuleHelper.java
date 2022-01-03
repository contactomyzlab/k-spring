package ve.zlab.core.helper.security.validator.model;

public class KRuleHelper {
    
    public static class SQLRule {
        
        public static SqlExistsByColumn assertExistsByColumn(final String table, final String column, final String message, final WhereProperty... whereProperties) {
            return new SqlExistsByColumn(table, column, message, whereProperties);
        }

        public static SqlNotExistsByColumn assertNotExistsByColumn(final String table, final String column, final String message) {
            return new SqlNotExistsByColumn(table, column, message);
        }

        public static SqlNotExistsByColumnExcludingById assertNotExistsByColumnExcludingById(final String table, final String column, final Long excluding, final String message) {
            return new SqlNotExistsByColumnExcludingById(table, column, excluding, message);
        }
        
        public static SqlNotIExistsByColumn assertNotIExistsByColumn(final String table, final String column, final String message) {
            return new SqlNotIExistsByColumn(table, column, message);
        }
    }

    public static class LongRule {

        public static LongMax assertMax(final Long max, final String message) {
            return new LongMax(max, message);
        }

        public static LongIn assertIn(final Long[] list, final String message) {
            return new LongIn(list, message);
        }

        public static LongNotNullNotZero assertNotNullNotZero(final String message) {
            return new LongNotNullNotZero(message);
        }

        public static LongBetween assertBetween(final Long from, final Long to, final String message) {
            return new LongBetween(from, to, message);
        }
    }

    public static class StringRule {

        public static StringNotNullNotEmpty assertNotNullNotEmpty(final String message) {
            return new StringNotNullNotEmpty(message);
        }

        public static StringParseableToDouble assertParseableToDouble(final String message) {
            return new StringParseableToDouble(message);
        }

        public static StringMax assertMax(final Integer max, final String message) {
            return new StringMax(max, message);
        }

        public static StringMin assertMin(final Integer min, final String message) {
            return new StringMin(min, message);
        }
        
        public static StringEmail assertEmailPattern(final String message) {
            return new StringEmail(message);
        }

        public static StringDateISO8601 assertDateISO8601(final String message) {
            return new StringDateISO8601(message);
        }
        
        public static StringOnlyLetters assertOnlyLetters(final String message) {
            return new StringOnlyLetters(message);
        }
        
        public static StringLettersWhiteSpaces assertLettersWhiteSpaces(final String message) {
            return new StringLettersWhiteSpaces(message);
        }
        
        public static StringLettersWhiteSpacesAccents assertLettersWhiteSpacesAccents(final String message) {
            return new StringLettersWhiteSpacesAccents(message);
        }
        
        public static StringOnlyNumbers assertOnlyNumbers(final String message) {
            return new StringOnlyNumbers(message);
        }
        
        public static StringAlphanumeric assertAlphanumeric(final String message) {
            return new StringAlphanumeric(message);
        }
        
        public static StringIn assertIn(final String[] list, final String message) {
            return new StringIn(list, message);
        }
    }

    public static class ObjectRule {

        public static ObjectNotNull assertNotNull(final String message) {
            return new ObjectNotNull(message);
        }

        public static ObjectNull assertNull(final String message) {
            return new ObjectNull(message);
        }
    }
    
    public static class DoubleRule {

        public static DoubleNotNull assertNotNull(final String message) {
            return new DoubleNotNull(message);
        }

        public static DoubleGreaterThan assertGreaterThan(final Double value, final String message) {
            return new DoubleGreaterThan(value, message);
        }
        
        public static DoubleGreaterThanOrEqualTo assertGreaterThanOrEqualTo(final Double value, final String message) {
            return new DoubleGreaterThanOrEqualTo(value, message);
        }
    }

    public static class IntegerRule {
        public static IntegerIn assertIn(final Integer[] list, final String message) {
            return new IntegerIn(list, message);
        }

        public static IntegerBetween assertBetween(final Integer from, final Integer to, final String message) {
            return new IntegerBetween(from, to, message);
        }

        public static IntegerGreaterThan assertGreaterThan(final Integer value, final String message) {
            return new IntegerGreaterThan(value, message);
        }

        public static IntegerGreaterThanOrEqualTo assertGreaterThanOrEqualTo(final Integer value, final String message) {
            return new IntegerGreaterThanOrEqualTo(value, message);
        }
    }

    public static class BooleanRule {

        public static BooleanFalse assertFalse(final String message) {
            return new BooleanFalse(message);
        }
    }

    public static class ListRule {

        public static ListNotEmpty assertNotEmpty(final String message) {
            return new ListNotEmpty(message);
        }

        public static ListMin assertMin(final Integer min, final String message) {
            return new ListMin(min, message);
        }
        
        public static ListMinOrEmpty assertMinOrEmpty(final Integer min, final String message) {
            return new ListMinOrEmpty(min, message);
        }
        
        public static class Property {
        
            public static class StringRule {

                public static ListPropertyStringParseableToDouble assertParseableToDouble(final String property, final String message) {
                    return new ListPropertyStringParseableToDouble(property, message);
                }
                
                public static ListPropertyStringDoubleBetween assertDoubleBetween(final String property, final Double from, final Double to, final String message) {
                    return new ListPropertyStringDoubleBetween(property, from, to, message);
                }
            }
            
            public static class SQLRule {

                public static ListPropertySqlAllExistsOn assertAllExistOn(final String table, final String column, final String message, final WhereProperty... whereProperties) {
                    return new ListPropertySqlAllExistsOn(table, column, message, whereProperties);
                }
            }
        }
    }
}
