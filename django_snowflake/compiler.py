from django.db.models.sql import compiler


class SQLCompiler(compiler.SQLCompiler):
    pass


class SQLDeleteCompiler(compiler.SQLDeleteCompiler, SQLCompiler):
    pass


class SQLInsertCompiler(compiler.SQLInsertCompiler, SQLCompiler):
    def as_sql(self):
        if 'JSONField' in [field.get_internal_type() for field in self.query.fields]:
            columns = [field.column for field in self.query.fields]
            insert_columns = ", ".join([f'{column}' for column in columns])
            insert_into = f'INSERT INTO {self.query.get_initial_alias()}({insert_columns})'
            select_columns = ", ".join([
                f"PARSE_JSON(Column{index + 1}) AS {field.column}"
                if field.get_internal_type() == "JSONField"
                else f"Column{index + 1} AS {field.column}"
                for index, field in enumerate(self.query.fields)
            ])
            select = f'SELECT {select_columns}'
            from_values = f'FROM VALUES ({", ".join("%s" for field in self.query.fields)})'

            result = f"{insert_into} {select} {from_values}"

            sql_with_params = self.query.sql_with_params()[0]
            return [(result, sql_with_params[1])]

        return super().as_sql()


class SQLUpdateCompiler(compiler.SQLUpdateCompiler, SQLCompiler):
    pass


class SQLAggregateCompiler(compiler.SQLAggregateCompiler, SQLCompiler):
    pass
