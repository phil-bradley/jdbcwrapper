/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ie.philb.jdbcwrapper.parameter;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author philb
 */
public class JdbcParameterSet {

    private final Set<JdbcParameter> parameters = new HashSet<>();

    public Set<JdbcParameter> getParameters() {
        return Collections.unmodifiableSet(parameters);
    }

    public void add(String key, Long value) {
        parameters.add(new LongJdbcParameter(key, value));
    }

    public void add(String key, Integer value) {
        parameters.add(new LongJdbcParameter(key, value));
    }

    public void add(String key, String value) {
        parameters.add(new StringJdbcParameter(key, value));
    }

    public void add(String key, BigDecimal value) {
        parameters.add(new BigDecimalJdbcParameter(key, value));
    }

    public void add(String key, Date value) {
        parameters.add(new DateJdbcParameter(key, value));
    }

    private boolean containsKey(String key) {
        for (JdbcParameter parameter : parameters) {
            if (parameter.getKey().equals(key)) {
                return true;
            }
        }

        return false;
    }
}
