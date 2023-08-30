{% macro get_normalized_data_type(exact_data_type) %}
    {% if exact_data_type is none %}
       {{ return (exact_data_type) }}
    {% endif %}
    {% set exact_data_type_uppercase = exact_data_type | upper %}
    {% set result = adapter.dispatch('get_normalized_data_type','elementary')(exact_data_type_uppercase) %}
    {{ return(result) }}
{% endmacro %}

{% macro default__get_normalized_data_type(exact_data_type) %}
   {{return (exact_data_type) }}
{% endmacro %}

{% macro bigquery__get_normalized_data_type(exact_data_type) %}
{# BigQuery has no concept of data type synonyms,
 see https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types #}
 {% set exact_data_type_to_data_type_returned_by_the_info_schema = {'BOOLEAN': 'BOOL'} %}
 {%- if exact_data_type in exact_data_type_to_data_type_returned_by_the_info_schema%}
   {{ return (exact_data_type_to_data_type_returned_by_the_info_schema[exact_data_type])}}
 {%- else %}
   {{return (exact_data_type) }}
 {%- endif%}
{% endmacro %}



{% macro snowflake__get_normalized_data_type(exact_data_type) %}
{# understanding Snowflake data type synonyms:
 https://docs.snowflake.com/en/sql-reference/intro-summary-data-types.html #}
 {% set exact_data_type_to_data_type_returned_by_the_info_schema = {'VARCHAR': 'TEXT',
                'STRING': 'TEXT',
                'NVARCHAR': 'TEXT',
                'NUMERIC': 'NUMBER',
                'DECIMAL': 'NUMBER',
                'INT':'NUMBER',
                'INTEGER':'NUMBER',
                'SMALLINT':'NUMBER',
                'BIGINT':'NUMBER',
                'TINYINT':'NUMBER',
                'BYTEINT':'NUMBER',
                'REAL': 'FLOAT',
                'DOUBLE':'FLOAT',
                'DOUBLE PRECISION': 'FLOAT'
                }%}
 {%- if exact_data_type in exact_data_type_to_data_type_returned_by_the_info_schema%}
   {{ return (exact_data_type_to_data_type_returned_by_the_info_schema[exact_data_type])}}
 {%- else %}
   {{return (exact_data_type) }}
 {%- endif%}
{% endmacro %}

{% macro exasol__get_normalized_data_type(exact_data_type) %}
{# understanding Exasol data type synonyms:
 https://docs.exasol.com/db/latest/sql_references/data_types/datatypealiases.htm #}
 {% set exact_data_type_to_data_type_returned_by_the_info_schema = {'VARCHAR': 'TEXT',
                'BIGINT': 'DECIMAL(36,0)',
                'BOOL': 'BOOLEAN',
                'CHAR': 'CHAR(1)',
                'CHARACTER': 'CHAR(1)',
                'CHARACTER LARGE OBJECT': 'VARCHAR(2000000)',
                'CLOB': 'VARCHAR(2000000)',
                'DEC': 'DECIMAL(18,0)',
                'DECIMAL': 'DECIMAL(18,0)',
                'DOUBLE':'	DOUBLE PRECISION',
                'HASHTYPE':'HASHTYPE (16 BYTE)',
                'FLOAT':'DOUBLE PRECISION',
                'INT':'DECIMAL(18,0)',
                'INTEGER':'DECIMAL(18,0)',
                'LONG VARCHAR':'VARCHAR(2000000)',
                'NUMBER': 'DOUBLE PRECISION',
                'NUMERIC':'DECIMAL(18,0)',
                'REAL': 'DOUBLE PRECISION',
                'SHORTINT':'	DECIMAL(9,0)',
                'SMALLINT': '	DECIMAL(9,0)',
                'TIMESTAMP':'TIMESTAMP(3)',
                'TIMESTAMP WITH LOCAL TIME ZONE':'TIMESTAMP(3) WITH LOCAL TIME ZONE',
                'TINYINT':'DECIMAL(3,0)'
                }%}
 {%- if exact_data_type in exact_data_type_to_data_type_returned_by_the_info_schema%}
   {{ return (exact_data_type_to_data_type_returned_by_the_info_schema[exact_data_type])}}
 {%- else %}
   {{return (exact_data_type) }}
 {%- endif%}
{% endmacro %}



{% macro spark__get_normalized_data_type(exact_data_type) %}
{# spark also has no concept of data type synonyms :
   see https://spark.apache.org/docs/latest/sql-ref-datatypes.html #}
   {{return (exact_data_type) }}
{% endmacro %}


{% macro redshift__get_normalized_data_type(exact_data_type) %}
{# understanding Redshift data type synonyms:
 https://docs.aws.amazon.com/redshift/latest/dg/c_Supported_data_types.html #}
 {% set exact_data_type_to_data_type_returned_by_the_info_schema = {'INT2':	'SMALLINT',
                                                                   'INT':	'INTEGER',
                                                                   'INT4':	'INTEGER',
                                                                   'INT8':	'BIGINT',
                                                                   'NUMERIC':	'DECIMAL',
                                                                   'FLOAT4':	'REAL',
                                                                   'FLOAT8':	'DOUBLE PRECISION',
                                                                   'FLOAT':	'DOUBLE PRECISION',
                                                                   'BOOL':	'BOOLEAN',
                                                                   'CHARACTER':	'CHAR',
                                                                   'NCHAR':	'CHAR',
                                                                   'BPCHAR':	'CHAR',
                                                                   'VARCHAR': 'CHARACTER VARYING',
                                                                   'NVARCHAR':	'CHARACTER VARYING',
                                                                   'TEXT':	'CHARACTER VARYING',
                                                                   'TIMESTAMP': 'TIMESTAMP WITHOUT TIME ZONE',
                                                                   'TIMESTAMPTZ': 'TIMESTAMP WITH TIME ZONE',
                                                                   'TIME': 'TIME WITHOUT TIME ZONE',
                                                                   'TIME WITH TIME ZONE':	'TIMETZ',
                                                                   'VARBINARY': 'BINARY VARYING',
                                                                   'VARBYTE': 'BINARY VARYING'} %}
 {%- if exact_data_type in exact_data_type_to_data_type_returned_by_the_info_schema%}
   {{ return (exact_data_type_to_data_type_returned_by_the_info_schema[exact_data_type])}}
 {%- else %}
   {{return (exact_data_type) }}
 {%- endif%}
{% endmacro %}


{% macro postgres__get_normalized_data_type(exact_data_type) %}
{# understanding Postgres data type synonyms:
 https://www.postgresql.org/docs/current/datatype.html #}
 {% set exact_data_type_to_data_type_returned_by_the_info_schema =  {'BIGINT': 'INT8',
                                                                    'BIGSERIAL': 'SERIAL8',
                                                                    'BIT VARYING': 'VARBIT',
                                                                    'BOOLEAN': 'BOOL',
                                                                    'CHARACTER': 'CHAR',
                                                                    'CHARACTER VARYING': 'VARCHAR',
                                                                    'DOUBLE PRECISION': 'FLOAT8',
                                                                    'INTEGER': 'INT, INT4',
                                                                    'NUMERIC': 'DECIMAL',
                                                                    'REAL': 'FLOAT4',
                                                                    'SMALLINT': 'INT2',
                                                                    'SMALLSERIAL': 'SERIAL2',
                                                                    'SERIAL': 'SERIAL4',
                                                                    'TIME WITH TIME ZONE': 'TIMETZ',
                                                                    'TIMESTAMP WITH TIME ZONE': 'TIMESTAMPTZ'} %}
 {%- if exact_data_type in exact_data_type_to_data_type_returned_by_the_info_schema%}
   {{ return (exact_data_type_to_data_type_returned_by_the_info_schema[exact_data_type])}}
 {%- else %}
   {{return (exact_data_type) }}
 {%- endif%}
{% endmacro %}
