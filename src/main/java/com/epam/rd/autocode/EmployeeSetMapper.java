package com.epam.rd.autocode;

import com.epam.rd.autocode.domain.Employee;
import com.epam.rd.autocode.domain.FullName;
import com.epam.rd.autocode.domain.Position;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.HashSet;
import java.util.Set;

public class EmployeeSetMapper implements SetMapper<Set<Employee>> {
    private static final byte COLUMN_ID = 1;
    private static final byte COLUMN_FIRST_NAME = 2;
    private static final byte COLUMN_LAST_NAME = 3;
    private static final byte COLUMN_MIDDLE_NAME = 4;
    private static final byte COLUMN_POSITION = 5;
    private static final byte COLUMN_MANAGER = 6;
    private static final byte COLUMN_HIREDATE = 7;
    private static final byte COLUMN_SALARY = 8;
    private BigInteger id;
    private FullName fullName;
    private Position position;
    private LocalDate hired;
    private BigDecimal salary;

    @Override
    public Set<Employee> mapSet(ResultSet resultSet) {
        Set<Employee> employeeSet = new HashSet<>();
        try {
            while (resultSet.next()) {
                id = new BigInteger(String.valueOf(resultSet.getLong(COLUMN_ID)));
                fullName = new FullName(
                    resultSet.getString(COLUMN_FIRST_NAME),
                    resultSet.getString(COLUMN_LAST_NAME),
                    resultSet.getString(COLUMN_MIDDLE_NAME));
                position = Position.valueOf(resultSet.getString(COLUMN_POSITION));
                hired = resultSet.getDate(COLUMN_HIREDATE).toLocalDate();
                salary = resultSet.getBigDecimal(COLUMN_SALARY);
                BigInteger managerId = new BigInteger(String.valueOf(resultSet.getLong(COLUMN_MANAGER)));
                int currentRow = resultSet.getRow();
                employeeSet.add(new Employee(id, fullName, position, hired, salary,
                    createEmployee(resultSet, managerId)));

                resultSet.absolute(currentRow);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return employeeSet;
    }

    public Employee createEmployee(ResultSet resultSet, BigInteger currentManagerId) throws SQLException {
        Employee newEmployee = null;
        resultSet.first();
        while (resultSet.next()) {
            id = new BigInteger(String.valueOf(resultSet.getLong(COLUMN_ID)));
            fullName = new FullName(
                resultSet.getString(COLUMN_FIRST_NAME),
                resultSet.getString(COLUMN_LAST_NAME),
                resultSet.getString(COLUMN_MIDDLE_NAME));
            position = Position.valueOf(resultSet.getString(COLUMN_POSITION));
            hired = resultSet.getDate(COLUMN_HIREDATE).toLocalDate();
            salary = resultSet.getBigDecimal(COLUMN_SALARY);
            BigInteger newManagerId = new BigInteger(String.valueOf(resultSet.getLong(COLUMN_MANAGER)));

            if (currentManagerId.equals(id)) {
                newEmployee = new Employee(id, fullName, position, hired, salary,
                    createEmployee(resultSet, newManagerId));
            }
        }
        return newEmployee;
    }
}
