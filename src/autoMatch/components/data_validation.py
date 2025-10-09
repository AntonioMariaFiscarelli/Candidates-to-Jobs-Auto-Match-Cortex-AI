from autoMatch import logger

from autoMatch.entity.config_entity import DataValidationConfig

class DataValidation:
    def __init__(self, config: DataValidationConfig):
        self.config = config


    def validate_all_columns(self, session)-> bool:
        try:
            validation_status = True

            database = self.config.database
            schema = self.config.schema
            input_table = self.config.input_table
            table_schema = self.config.table_schema
            status_file = self.config.status_file

            for input_table, intput_table_snowflake in input_table.items():
                schema_check = True
                df = session.table(f"{database}.{schema}.{intput_table_snowflake}")

                columns = table_schema[input_table].columns
                df_cols = [col.lower() for col in df.columns]
                schema_cols = [col.lower() for col in columns.keys()]

                missing_columns = list(set(schema_cols) - set(df_cols))
                extra_columns = list(set(df_cols) - set(schema_cols))

                for col in missing_columns:
                    validation_status = False
                    schema_check = False
                    logger.info(f"Column {col} from schema is missing in the dataframe")

                for col in extra_columns:
                    logger.info(f"Column {col} is present in the dataframe but not specified in the schema")

                for field in df.schema.fields:
                    for col_name, col_type in columns.items():
                        if col_name == field.name.lower():
                            if col_type not in str(field.datatype).lower():
                                validation_status = False
                                schema_check = False
                                logger.info(f"Dataframe column {col_name} with type ({col_type}) does not match schema column type {field.name} ({field.datatype})")

                logger.info(f"Validation check for table {intput_table_snowflake}: {'SUCCESS' if schema_check else 'FAIL'}")
                with open(status_file, 'w') as f:
                    f.write(f"Validation status: {validation_status}")

            return validation_status
        
        except Exception as e:
            raise e