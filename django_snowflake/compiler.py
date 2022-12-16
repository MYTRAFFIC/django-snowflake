from django.db.models.sql import compiler


class SQLCompiler(compiler.SQLCompiler):
    pass


class SQLDeleteCompiler(compiler.SQLDeleteCompiler, SQLCompiler):
    pass


class SQLInsertCompiler(compiler.SQLInsertCompiler, SQLCompiler):
    # List of fields' internal type that should be parsed as JSON.
    # You can add to this list to support e.g. Array.
    json_fields = {"JSONField"}

    def as_sql(self):
        """
        When inserting VARIANT, OBJECT or ARRAY values, the "INSERT INTO ... SELECT"
        format is required. PARSE_JSON(), TO_VARIANT() and other functions like
        these cannot be used in "INSERT INTO ... VALUES (...)". Read more:
        https://docs.snowflake.com/en/sql-reference/data-types-semistructured.html#example-of-inserting-a-variant

        Ex:
        INSERT INTO my_table(col1, col2, col3)
        VALUES (TO_VARIANT(val1), PARSE_JSON(val2), val3)

         |
         |  becomes
         V

        INSERT INTO my_table(col1, col2, col3)
        SELECT
            TO_VARIANT(Column1) as col1,
            PARSE_JSON(Column2) as col2,
            Column3 as col3
        FROM VALUES (val1, val2, val3)

        To simplify things, as Snowflake doesn't support returning fields, we
        use this form for all inserts.
        """
        # We don't need quote_name_unless_alias() here, since these are all
        # going to be column names (so we can avoid the extra overhead).
        qn = self.connection.ops.quote_name
        opts = self.query.get_meta()
        insert_statement = self.connection.ops.insert_statement(
            ignore_conflicts=self.query.ignore_conflicts
        )
        result = ["%s %s" % (insert_statement, qn(opts.db_table))]
        fields = self.query.fields or [opts.pk]
        result.append("(%s)" % ", ".join(qn(f.column) for f in fields))

        # Custom part, we add an intermediary SELECT
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

        select_columns = ", ".join([
            f"PARSE_JSON(Column{index + 1}) AS {field.column}"
            if field.get_internal_type() in self.json_fields
            else f"Column{index + 1} AS {field.column}"
            for index, field in enumerate(self.query.fields)
        ])
        select = f'SELECT {select_columns} FROM'
        result.append(select)

        # End of custom part
        #
        # After this, we only keep the code for bulk insert for simplification.

        value_rows = [
            [
                self.prepare_value(field, self.pre_save_val(field, obj))
                for field in fields
            ]
            for obj in self.query.objs
        ]
        placeholder_rows, param_rows = self.assemble_as_sql(fields, value_rows)

        result.append(self.connection.ops.bulk_insert_sql(fields, placeholder_rows))

        ignore_conflicts_suffix_sql = self.connection.ops.ignore_conflicts_suffix_sql(
            ignore_conflicts=self.query.ignore_conflicts
        )
        if ignore_conflicts_suffix_sql:
            result.append(ignore_conflicts_suffix_sql)
        return [(" ".join(result), tuple(p for ps in param_rows for p in ps))]


class SQLUpdateCompiler(compiler.SQLUpdateCompiler, SQLCompiler):
    pass


class SQLAggregateCompiler(compiler.SQLAggregateCompiler, SQLCompiler):
    pass
