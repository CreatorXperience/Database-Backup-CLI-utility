from importlib import resources

try:
    import tomllib
except ModuleNotFoundError as MotherFker:
    import tomli as tomllib

__all__ = [""]
__version__ = "1.0.0"


config_dict = tomllib.load(resources.read_text("db.bkup", "config.toml"))

config_dict["section"]["example"]
