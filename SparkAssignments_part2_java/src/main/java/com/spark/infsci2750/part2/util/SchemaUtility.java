package com.spark.infsci2750.part2.util;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class SchemaUtility {

    public static StructType getStructTypeForSchema(String schemaString, String delimiter){
        List<StructField> fields = new ArrayList<>();
        for (String fieldName : schemaString.split(delimiter)) {
            StructField field = DataTypes.createStructField(fieldName.trim(), DataTypes.StringType, true);
            fields.add(field);
        }
        return  DataTypes.createStructType(fields);
    }
}
