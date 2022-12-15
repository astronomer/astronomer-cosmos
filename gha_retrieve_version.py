def get_version():
    import configparser
    config = configparser.RawConfigParser()
    config.read_file(open(r'setup.cfg'))
    version = config.get('metadata', 'version')
    return version

if __name__ == '__main__':
    version = get_version()
    print(version)
