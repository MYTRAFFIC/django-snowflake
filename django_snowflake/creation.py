import sys

from django.db.backends.base.creation import BaseDatabaseCreation


class DatabaseCreation(BaseDatabaseCreation):
    def _quote_name(self, name):
        return self.connection.ops.quote_name(name)

    def _database_exists(self, cursor, database_name):
        try:
            cursor.execute(f'USE DATABASE {database_name}')
        except Exception as exc:
            if 'Object does not exist, or operation cannot be performed.' in str(exc):
                return False
            raise
        return True

    def _execute_create_test_db(self, cursor, parameters, keepdb=False):
        dbname = parameters['dbname']
        if not keepdb or not self._database_exists(cursor, dbname):
            # Try to create a database if keepdb=False or if keepdb=True and
            # the database doesn't exist.
            super()._execute_create_test_db(cursor, parameters, keepdb)
            cursor.execute(f'USE DATABASE {dbname}')

        schema_name = parameters.get('schema_name')
        if schema_name:
            schema_name = self._quote_name(schema_name)
            cursor.execute(f'CREATE SCHEMA IF NOT EXISTS {schema_name}')
            cursor.execute(f'USE SCHEMA {schema_name}')

    def _create_test_db(self, verbosity, autoclobber, keepdb=False):
        """
        Internal implementation - create the test db tables.
        """
        test_database_name = self._get_test_db_name()

        schema_name = self.connection.get_connection_params()['test_schema']
        test_db_params = {
            "dbname": self.connection.ops.quote_name(test_database_name),
            "schema_name": self._quote_name(schema_name),
            "suffix": self.sql_table_creation_suffix(),
        }
        # Create the test database and connect to it.
        with self._nodb_cursor() as cursor:
            if not keepdb:
                cursor.execute("DROP SCHEMA %(schema_name)s" % test_db_params)
            self._execute_create_test_db(cursor, test_db_params, keepdb)

        return test_database_name

    def _clone_test_db(self, suffix, verbosity, keepdb=False):
        source_database_name = self.connection.settings_dict['NAME']
        schema_name = self.connection.get_connection_params()['test_schema']
        target_database_name = self.get_test_db_clone_settings(suffix)['NAME']

        test_db_params = {
            'dbname': self._quote_name(target_database_name),
            'schema_name': self._quote_name(schema_name),
            'suffix': 'CLONE ' + self._quote_name(source_database_name),
        }
        with self._nodb_cursor() as cursor:
            try:
                self._execute_create_test_db(cursor, test_db_params, keepdb)
            except Exception:
                try:
                    if verbosity >= 1:
                        self.log('Destroying old test schema for alias %s...' % (
                            self._get_database_display_str(verbosity, schema_name),
                        ))
                    cursor.execute('DROP SCHEMA %(schema_name)s' % test_db_params)
                    self._execute_create_test_db(cursor, test_db_params, keepdb)
                except Exception as e:
                    self.log('Got an error cloning the test schema: %s' % e)
                    sys.exit(2)
