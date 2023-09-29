import os
import argparse
import configparser
import sqlite3

DEFAULT_CONFIG_DIR = os.path.expanduser("~/.pegasus")
DEFAULT_DB_PATH = os.path.join(DEFAULT_CONFIG_DIR, "workflow.db")

class ConfigGenerator:
    def __init__(self, db_path=DEFAULT_DB_PATH):
        self.db_path = db_path
        self.connection = self.create_db_connection()

    def create_db_connection(self):
        connection = sqlite3.connect(self.db_path)
        return connection

    def create_config_table(self):
        cursor = self.connection.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS pegasus_data_config (
                id INTEGER PRIMARY KEY,
                section_name TEXT NOT NULL,
                setting_name TEXT NOT NULL,
                setting_value TEXT NOT NULL
            )
        ''')
        self.connection.commit()

    def update_config_in_db(self, section_name, setting_name, setting_value):
        cursor = self.connection.cursor()
        cursor.execute('UPDATE pegasus_data_config SET setting_value = ? WHERE section_name = ? AND setting_name = ?',
                       (setting_value, section_name, setting_name))
        self.connection.commit()

    def save_config_to_db(self, section_name, setting_name, setting_value):
        existing_value = self.get_config_from_db(section_name, setting_name)
        if existing_value is not None:
            self.update_config_in_db(section_name, setting_name, setting_value)
        else:
            cursor = self.connection.cursor()
            cursor.execute('INSERT INTO pegasus_data_config (section_name, setting_name, setting_value) VALUES (?, ?, ?)',
                           (section_name, setting_name, setting_value))
            self.connection.commit()

    def get_config_from_db(self, section_name, setting_name):
        cursor = self.connection.cursor()
        cursor.execute('SELECT setting_value FROM pegasus_data_config WHERE section_name = ? AND setting_name = ?',
                       (section_name, setting_name))
        result = cursor.fetchone()
        if result:
            return result[0]
        return None

    def generate_config_file(self, config_filename, args):
        config = configparser.ConfigParser()

        if args.use_mlflow:
            config["MLflow"] = {
                "tracking_uri": args.mlflow_uri,
                "auth_type": args.mlflow_auth_type,
                "username": args.username,
                "password": args.password,
                "experiment_name": args.experiment_name
            }
            self.save_config_to_db("MLflow", "tracking_uri", args.mlflow_uri)
            self.save_config_to_db("MLflow", "auth_type", args.mlflow_auth_type)
            self.save_config_to_db("MLflow", "username", args.username)
            self.save_config_to_db("MLflow", "password", args.password)
            self.save_config_to_db("MLflow", "experiment_name", args.experiment_name)

        if args.use_bucket:
            config["Bucket"] = {
                "bucket_name": args.bucket,
                "bucket_path": args.bucket_path,
                "credentials_file": args.credentials_file
            }
            self.save_config_to_db("Bucket", "bucket_name", args.bucket)

        if args.use_gdrive:
            config["GoogleDrive"] = {
                "gdrive_credentials_file": args.gdrive_credentials_file,
                "gdrive_parent_folder": args.gdrive_parent_folder
            }
            self.save_config_to_db("GoogleDrive", "gdrive_credentials_file", args.gdrive_credentials_file)
            self.save_config_to_db("GoogleDrive", "gdrive_parent_folder", args.gdrive_parent_folder)


        # create meta data file
        if not os.path.exists(args.metadata_file):
            with open(args.metadata_file, 'w') as file:
                pass  # This does nothing, effectively creating an empty 
            print(f"Empty file {args.metadata_file} created.")
        else:
            print(f"File '{args.metadata_file}' already exists.")
        
        config["metadata"] = {
                "path": args.metadata_file,
            }
          
        with open(config_filename, 'w') as configfile:
            config.write(configfile)
        
        if os.path.abspath(config_filename) != os.path.join(DEFAULT_CONFIG_DIR, "pegasus_data_config.conf"):
            config = configparser.ConfigParser()
            if os.path.exists(os.path.join(DEFAULT_CONFIG_DIR, "pegasus_data_config.conf")):
                # Read the configuration
                config.read(os.path.join(DEFAULT_CONFIG_DIR, "pegasus_data_config.conf"))
                
                if config.has_section('pegasusData') and config.has_option('pegasusData', 'path'):
                    # Read the current value
                    current_value = config.get('pegasusData', 'path')
                    print(f"Current configuration value: {current_value}")

                    # Modify the value
                    config.set('pegasusData', 'path',  os.path.abspath(config_filename))

                    # Write the modified configuration back to the file
                    with open(os.path.join(DEFAULT_CONFIG_DIR, "pegasus_data_config.conf"), 'w') as configfile:
                        config.write(config)
            else:
                    config["pegasusData"] = {
                        "path":  os.path.abspath(config_filename)
                    }              
                    with open(os.path.join(DEFAULT_CONFIG_DIR, "pegasus_data_config.conf"), 'w') as configfile:
                            config.write(configfile)            
            
                                           

    def close_connection(self):
        self.connection.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate a configuration file")
    parser.add_argument("--config-file", default=os.path.join(DEFAULT_CONFIG_DIR, "pegasus_data_config.conf"), help="Name of the configuration file to create")
    parser.add_argument("--metadata-file", default="metadata.yaml", help="Name of the metadata file to create")


    # MLflow configuration
    parser.add_argument("--use-mlflow", action="store_true", help="Use MLflow configuration")
    parser.add_argument("--mlflow-uri", default="http://localhost:5000", help="MLflow tracking URI")
    parser.add_argument("--mlflow-auth-type", choices=["username_password", "token", "non_auth"], default="non_auth", help="MLflow authentication type")
    parser.add_argument("--username", default="", help="MLflow username for authentication")
    parser.add_argument("--password", default="", help="MLflow password for authentication")
    parser.add_argument("--experiment-name", default="default_experiment", help="MLflow experiment name")

    # Bucket configuration
    parser.add_argument("--use-bucket", action="store_true", help="Use bucket configuration")
    parser.add_argument("--bucket", default="default_bucket", help="Bucket name")
    parser.add_argument("--bucket-path", default="", help="bucket path")
    parser.add_argument("--credentials-file", default="", help="Path to the file containing bucket credentials")

    # Google Drive configuration
    parser.add_argument("--use-gdrive", action="store_true", help="Use Google Drive configuration")
    parser.add_argument("--gdrive-credentials-file", default="", help="Path to the file containing Google Drive service account credentials")
    parser.add_argument("--gdrive-parent-folder", default="", help="Parent folder ID for Google Drive files")




    args = parser.parse_args()

    config_generator = ConfigGenerator()
    config_generator.create_config_table()
    config_generator.generate_config_file(args.config_file, args)
    config_generator.close_connection()
