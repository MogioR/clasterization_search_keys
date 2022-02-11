from Modules.Xml_river.xml_river import XmlRiver


class YandexXmlRiver(XmlRiver):
    def __init__(self, config):
        super().__init__(config)
        self.loc = config['XMLRiver']['Yandex']['default_loc_id']
        self.lang = config['XMLRiver']['Yandex']['default_language_id']
        self.use_lang = config['XMLRiver']['Yandex']['default_use_language']
        self.device = config['XMLRiver']['Yandex']['default_device']

    # Set locale for search
    def set_region(self, locale):
        if locale == 'MOSCOW':
            self.loc = 213
        elif locale == 'MINSK':
            self.loc = 149
        elif locale == 'KIEV':
            self.loc = 187

    # Change location of search
    def set_loc(self, loc_id):
        self.loc = loc_id

    # Return request to xml river api with keys
    def get_request(self, keys):
        return 'http://xmlriver.com/search_yandex/xml?user=' + self.xml_river_user+'&key=' + self.xml_river_key +\
               '&query=' + keys + '&groupby=' + str(self.group_by) + '&lr=' + str(self.loc) + \
               ('&lang='+self.lang if self.use_lang else '') + '&device=' + self.device
