package com.etl.possale;

import lombok.Data;

import java.io.Serializable;

@Data
public class SqlAndTable implements Serializable {

    private static final long serialVersionUID = -6824113160761871070L;

    private String sql;
    private String tableName;
    private String structName;
}
