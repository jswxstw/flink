package org.apache.flink.formats.json.custom.canal;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.formats.json.JsonFormatOptions;

/** Option utils for canal-json format. */
@PublicEvolving
public class CanalJsonFormatOptions {

    public static final ConfigOption<Boolean> IGNORE_PARSE_ERRORS =
            JsonFormatOptions.IGNORE_PARSE_ERRORS;

    public static final ConfigOption<String> TIMESTAMP_FORMAT = JsonFormatOptions.TIMESTAMP_FORMAT;

    public static final ConfigOption<String> JSON_MAP_NULL_KEY_MODE =
            JsonFormatOptions.MAP_NULL_KEY_MODE;

    public static final ConfigOption<String> JSON_MAP_NULL_KEY_LITERAL =
            JsonFormatOptions.MAP_NULL_KEY_LITERAL;

    public static final ConfigOption<String> DATABASE_INCLUDE =
            ConfigOptions.key("database.include")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "An optional regular expression to only read the specific databases changelog rows by regular matching the \"database\" meta field in the Canal record."
                                    + "The pattern string is compatible with Java's Pattern.");

    public static final ConfigOption<String> TABLE_INCLUDE =
            ConfigOptions.key("table.include")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "An optional regular expression to only read the specific tables changelog rows by regular matching the \"table\" meta field in the Canal record."
                                    + "The pattern string is compatible with Java's Pattern.");

    private CanalJsonFormatOptions() {}
}
