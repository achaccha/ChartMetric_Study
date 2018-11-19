from dotenv import load_dotenv, find_dotenv
import os

load_dotenv(find_dotenv())

class Config:

    postgres = {
        'dbname' : os.environ.get("DB_NAME"),
        'user' : os.environ.get("USER"),
        'host' : os.environ.get("HOST"),
        'password' : os.environ.get("PASSWORD")
    }

    table = os.environ.get("TABLE")
