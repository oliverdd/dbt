{% macro refresh_pipe(pipe_name) %}

        alter pipe {{pipe_name}} refresh
        
{% endmacro %}