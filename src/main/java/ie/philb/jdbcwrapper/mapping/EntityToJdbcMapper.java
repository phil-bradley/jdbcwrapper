/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ie.philb.jdbcwrapper.mapping;

import ie.philb.jdbcwrapper.parameter.JdbcParameterSet;

/**
 *
 * @author philb
 */
public interface EntityToJdbcMapper<T> {

    public JdbcParameterSet getParameterSet(T t);

}
