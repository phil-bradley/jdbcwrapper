/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ie.philb.jdbcwrapper.exception;

/**
 *
 * @author pbradley
 */
public class JdbcWrapperException extends Exception {
    
    private final String sql;
    
    public JdbcWrapperException(String sql, Throwable ex) {
        super("SQL Failed  -->" + sql + "<--", ex);
        this.sql = sql;
    }
    
    public String getSql() {
        return sql;
    }
}
