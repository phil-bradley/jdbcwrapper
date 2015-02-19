/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ie.philb.jdbcwrapper.parameter;

/**
 *
 * @author pbradley
 */
public class StringJdbcParameter extends JdbcParameter<String> {

    private final String value;

    public StringJdbcParameter(String key, String value) {
        super(key);
        this.value = value;
    }

//    @Override
//    public String getValueAsString() {
//        return value;
//    }
//
//    @Override
//    public String getEscapedValue() {
//        return getValueAsString().replaceAll("'", "''");
//    }
//
//    @Override
//    public String getValueForEmbedding() {
//        return "'" + getEscapedValue() + "'";
//    }

    @Override
    public String getValue() {
        return value;
    }
}
