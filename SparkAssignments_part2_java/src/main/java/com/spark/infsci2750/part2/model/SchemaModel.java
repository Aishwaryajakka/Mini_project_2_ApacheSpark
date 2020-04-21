package com.spark.infsci2750.part2.model;

import java.io.Serializable;

public class SchemaModel implements Serializable {

    public String schemaString;
    public String schemaDelimiter;

    public String getSchemaString() {
        return schemaString;
    }

    public void setSchemaString(String schemaString) {
        this.schemaString = schemaString;
    }

    public String getSchemaDelimiter() {
        return schemaDelimiter;
    }

    public void setSchemaDelimiter(String schemaDelimiter) {
        this.schemaDelimiter = schemaDelimiter;
    }
}
