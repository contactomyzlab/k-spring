package ve.zlab.core.helper.security.validator.model;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;
import ve.zlab.k.helper.KExceptionHelper;
import ve.zlab.k.KException;
import ve.zlab.k.KExecutor;

public class PayloadValidator {

    protected Map<String, KRule[]> map;
    
    public PayloadValidator() {
        this.map = new HashMap<>();
    }

    public PayloadValidator(final Map<String, KRule[]> rules) {
        this.map = rules;
    }
    
    public void put(final String name, final KRule[] rule) {
        if (this.map.containsKey(name)) {
            this.map.put(name, ArrayUtils.addAll(this.map.get(name), rule));
        } else {
            this.map.put(name, rule);
        }
    }

    public KRule[] get(final String property) {
        return map.get(property);
    }

    public void validate(final KExecutor K) throws KException {
        for (final Map.Entry<String, KRule[]> entry : map.entrySet()) {
            final String property = entry.getKey();
            final KRule[] rules = entry.getValue();
            
            final String endName;

            if (property.length() > 1) {
                endName = property.substring(1);
            } else {
                endName = "";
            }

            final Method method;
            final Object value;

            try {
                method = this.getClass().getMethod(StringUtils.join(new String[]{
                    "get", property.substring(0, 1).toUpperCase(), endName
                }, ""), new Class<?>[]{});

                value = method.invoke(this, new Object[]{});
            } catch (Exception e) {
                e.printStackTrace();
                
                throw KExceptionHelper.internalServerError();
            }

            for (final KRule rule : rules) {
                switch (rule.getRule()) {
                    case 1: //KRule.LONG_MAX
                        ((LongMax) rule).validate((Long) value);
                        break;
                    case 2: //KRule.STRING_NOT_NULL_NOT_EMPTY
                        ((StringNotNullNotEmpty) rule).validate((String) value);
                        break;
                    case 3: //KRule.STRING_PARSEABLE_TO_DOUBLE
                        ((StringParseableToDouble) rule).validate((String) value);
                        break;
                    case 4: //KRule.LONG_IN
                        ((LongIn) rule).validate((Long) value);
                        break;
                    case 5: //KRule.OBJECT_NOT_NULL
                        ((ObjectNotNull) rule).validate(value);
                        break;
                    case 6: //KRule.INTEGER_BETWEEN
                        ((IntegerBetween) rule).validate((Integer) value);
                        break;
                    case 7: //KRule.LONG_NOT_NULL_NOT_ZERO
                        ((LongNotNullNotZero) rule).validate((Long) value);
                        break;
                    case 8: //KRule.STRING_MAX
                        ((StringMax) rule).validate((String) value);
                        break;
                    case 9: //KRule.SQL_EXISTS_BY_COLUMN 
                        ((SqlExistsByColumn) rule).validate(K, value);
                        break;
                    case 10: //KRule.BOOLEAN_FALSE 
                        ((BooleanFalse) rule).validate((boolean) value);
                        break;
                    case 11: //KRule.OBJECT_NULL
                        ((ObjectNull) rule).validate(value);
                        break;
                    case 12: //KRule.LONG_BETWEEN
                        ((LongBetween) rule).validate((Long) value);
                        break;
                    case 13: //KRule.EMAIL_PATTERN
                        ((StringEmail) rule).validate((String) value);
                        break;
                    case 14: //KRule.SQL_NOT_EXISTS_BY_COLUMN
                        ((SqlNotExistsByColumn) rule).validate(K, value);
                        break;
                    case 15: //KRule.INTEGER_GREATHER_THAN
                        ((IntegerGreaterThan) rule).validate((Integer) value);
                        break;
                    case 16: //KRule.LIST_NOT_EMPTY
                        ((ListNotEmpty) rule).validate((List) value);
                        break;
                    case 17: //KRule.LIST_NOT_EMPTY
                        ((SqlNotExistsByColumnExcludingById) rule).validate(K, value);
                        break;
                    case 18: //KRule.INTEGER_GREATHER_THAN_OR_EQUAL_TO
                        ((SqlNotExistsByColumnExcludingById) rule).validate(K, value);
                        break;
                    case 19: //KRule.DATE_ISO8601
                        ((StringDateISO8601) rule).validate((String) value);
                        break;
                    case 20: //KRule.LIST_MIN
                        ((ListMin) rule).validate((List) value);
                        break;
                    case 21: //KRule.LIST_PROPERTY_STRING_PARSEABLE_TO_DOUBLE
                        final List<Object> list21 = (List<Object>) value;
                        
                        for (final Object object : list21) {
                            ((ListPropertyStringParseableToDouble) rule).validate(object);
                        }
                        
                        break;
                    case 22: //KRule.LIST_PROPERTY_STRING_DOUBLE_BETWEEN
                        final List<Object> list22 = (List<Object>) value;
                        
                        for (final Object object : list22) {
                            ((ListPropertyStringDoubleBetween) rule).validate(object);
                        }
                        
                        break;
                    case 23: //KRule.LIST_MIN_OR_EMPTY
                        ((ListMinOrEmpty) rule).validate((List) value);
                        break;
                    case 24: //KRule.DOUBLE_NOT_NULL
                        ((DoubleNotNull) rule).validate((Double) value);
                        break;
                    case 25: //KRule.DOUBLE_GREATER_THAN
                        ((DoubleGreaterThan) rule).validate((Double) value);
                        break;
                    case 26: //KRule.LIST_ALL_EXISTS_ON
                        ((ListPropertySqlAllExistsOn) rule).validate(K, (List<Object>) value);
                        break;
                    case 27: //KRule.STRING_MIN
                        ((StringMin) rule).validate((String) value);
                        break;
                    case 28: //KRule.STRING_ONLY_LETTERS
                        ((StringOnlyLetters) rule).validate((String) value);
                        break;
                    case 29: //KRule.STRING_LETTERS_WHITESPACES
                        ((StringLettersWhiteSpaces) rule).validate((String) value);
                        break;
                    case 30: //KRule.SQL_NOT_IEXISTS_BY_COLUMN
                        ((SqlNotIExistsByColumn) rule).validate(K, (String) value);
                        break;
                    case 31: //KRule.STRING_LETTERS_WHITESPACES_ACENTS
                        ((StringLettersWhiteSpacesAccents) rule).validate((String) value);
                        break;
                    case 32: //KRule.STRING_ONLY_NUMBERS
                        ((StringOnlyNumbers) rule).validate((String) value);
                        break;
                    case 33: //KRule.STRING_ALPHANUMERIC
                        ((StringAlphanumeric) rule).validate((String) value);
                        break;
                    case 34: //KRule.INTEGER_IN
                        ((IntegerIn) rule).validate((Integer) value);
                        break;
                    case 35: //KRule.STRING_IN
                        ((StringIn) rule).validate((String) value);
                        break;
                    case 36: //KRule.DOUBLE_GREATER_THAN_OR_EQUAL_TO
                        ((DoubleGreaterThanOrEqualTo) rule).validate((Double) value);
                        break;
                    case 37: //KRule.LIST_PROPERTY_OBJECT_NOT_NULL
                        final List<Object> list37 = (List<Object>) value;
                        
                        for (final Object object : list37) {
                            ((ListPropertyObjectNotNull) rule).validate(object);
                        }
                        
                        break;
                    default:
                }
            }
        }
    }

    public void validate() throws KException {
        this.validate(null);
    }
    
    public void stage() {
        
    }
    
    public void stage(final HashMap<String, Object> data) {
        
    }
    
    public String toJSON() {
        return new JSONObject(this).toString();
    }
}
