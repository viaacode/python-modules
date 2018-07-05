import configparser


class Config:
    def __init__(self, config=None, section=None):
        if config is None:
            config = 'config.ini'
        if type(config) is str:
            config_file = config
            config = configparser.ConfigParser()
            config.read(config_file)
        if section and section in config:
            config = config[section]

        self.config = config

    def __getattr__(self, name):
        if not self.__contains__(name):
            raise KeyError(name)
        return self.config[name]

    def __contains__(self, key):
        return key in self.config

    def __getitem__(self, key):
        if not self.__contains__(key):
            raise IndexError("Invalid item '%s'" % key)
        return self.config[key]
