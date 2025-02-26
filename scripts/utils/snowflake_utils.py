import snowflake.connector

def get_snowflake_connection(config):
    """Create Snowflake connection from config"""
    try:
        return snowflake.connector.connect(
            account=config['ACCOUNT'],
            user=config['USER'],
            authenticator=config['AUTHENTICATOR'],
            warehouse=config['WAREHOUSE'],
            role=config['ROLE'],
            database=config['DATABASE']
        )
    except Exception as e:
        raise Exception(f"Failed to connect to Snowflake: {str(e)}")

def get_table_columns(conn, database, schema, table):
    """Get column information for a table"""
    try:
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT COLUMN_NAME
            FROM {database}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema}'
            AND TABLE_NAME = '{table}'
            ORDER BY ORDINAL_POSITION
        """)
        return cursor.fetchall()
    finally:
        if cursor:
            cursor.close() 