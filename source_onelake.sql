{% macro source_onelake(source_name, table_name) -%}
{#
    Generates an OPENROWSET statement for reading files from OneLake.
    
    Args:
        source_name: Name of the source (e.g., 'jaffle_shop')
        table_name: Name of the table in source (e.g., 'orders')
    
    Usage in model:
        {{ source_onelake('anlb', 'anlb_beheerpakket') }}
    
    Requires in dbt source definition:
        - columns with data_type property
        - meta.onelake block with these required fields:
          Source level (applies to all tables):
            onelake_path: base OneLake URL
            target_workspace: workspace GUID
            target_lakehouse: lakehouse GUID
            files_root: root folder (default: Files)
          
          Table level:
            filename: exact filename in OneLake
            folder: subfolder path
            format: CSV, Parquet, etc. (default: CSV)
            first_row: skip rows (default: 2)
            field_terminator: delimiter (default: ;)
            row_terminator: line ending (default: 0x0A)
    
    Troubleshooting:
    - "not authorized" or "Accessing storage path ... is not supported":
        OneLake paths are CASE SENSITIVE. Verify folder and filename match exactly.
    - "file not found":
        Check the constructed path and casing in logs matches OneLake.
#}

{% if not execute %}
    {{ return('') }}
{% endif %}

{# Get source configuration #}
{% set src_node = graph.sources.values() | selectattr('source_name', 'equalto', source_name) | selectattr('name', 'equalto', table_name) | first %}
{% set source_meta = src_node.source_meta or {} %}
{% set table_meta = src_node.meta or {} %}
{% set source_onelake_meta = source_meta.get('onelake', {}) %}
{% set table_onelake_meta = table_meta.get('onelake', {}) %}

{# Get OneLake base configuration #}
{% set onelake_path = source_onelake_meta.get('onelake_path') %}
{% set target_workspace = source_onelake_meta.get('target_workspace') %}
{% set target_lakehouse = source_onelake_meta.get('target_lakehouse') %}
{% set files_root = source_onelake_meta.get('files_root') or 'Files' %}

{% if not onelake_path or not target_workspace or not target_lakehouse %}
    {{ exceptions.raise_compiler_error("source_onelake: Missing required metadata (onelake_path, target_workspace, target_lakehouse) for '" ~ source_name ~ "'.") }}
{% endif %}

{# Build base path #}
{% set base_path = onelake_path.rstrip('/') ~ '/' ~ target_workspace ~ '/' ~ target_lakehouse ~ '/' ~ files_root.strip('/') %}
{% set base_path = base_path.rstrip('/') %}

{# Get filename and folder #}
{% set filename = table_onelake_meta.get('filename') %}
{% set folder = table_onelake_meta.get('folder') or '' %}
{% set folder = folder.strip('/') %}

{% if not filename %}
    {{ exceptions.raise_compiler_error("source_onelake: Missing 'filename' in meta.onelake for '" ~ source_name ~ "." ~ table_name ~ "'.") }}
{% endif %}

{% set relative_path = folder ~ '/' ~ filename if folder else filename %}
{% set bulk_path = base_path ~ '/' ~ relative_path %}

{# Get format and CSV settings #}
{% set format = (table_onelake_meta.get('format') or 'CSV') | upper %}

{{- log('Reading ' ~ format ~ ' ' ~ source_name ~ '.' ~ table_name ~ ' from ' ~ bulk_path, info=True) -}}
{% set first_row = table_onelake_meta.get('first_row') or 2 %}
{% set field_terminator = table_onelake_meta.get('field_terminator') or ';' %}
{% set row_terminator = table_onelake_meta.get('row_terminator') or '0x0A' %}

{# Get column definitions from source #}
{% set columns = src_node.columns.values() | list %}

{# Build OPENROWSET options based on format #}
{% if format == 'CSV' %}
    {% set openrowset_options = [
        "BULK '" ~ bulk_path ~ "'",
        "FORMAT = '" ~ format ~ "'",
        "FIRSTROW = " ~ first_row,
        "FIELDTERMINATOR = '" ~ field_terminator ~ "'",
        "ROWTERMINATOR = '" ~ row_terminator ~ "'",
        "MAXERRORS = 0"
    ] %}
{% else %}
    {% set openrowset_options = [
        "BULK '" ~ bulk_path ~ "'",
        "FORMAT = '" ~ format ~ "'"
    ] %}
{% endif %}

{% set openrowset_clause = openrowset_options | join(',\n    ') %}

{# Build column definitions, quoting names with spaces #}
{% set column_definitions = [] %}
{% for col in columns %}
    {% set col_name = col.name %}
    {% if ' ' in col_name %}
        {% set col_name = '[' ~ col_name ~ ']' %}
    {% endif %}
    {% do column_definitions.append('    ' ~ col_name ~ ' ' ~ col.data_type ~ (',' if not loop.last else '')) %}
{% endfor %}

{% set sql -%}
OPENROWSET(
    {{ openrowset_clause }}
)
WITH (
{{ column_definitions | join('\n') }}
)
{%- endset %}

{{ return(sql | trim) }}


{%- endmacro %}
