from Modules.Xml_river.xml_river import XmlRiver


class GoogleXmlRiver(XmlRiver):
    def __init__(self, config):
        super().__init__(config)
        self.country = config['XMLRiver']['Google']['default_country_id']
        self.loc = config['XMLRiver']['Google']['default_loc_id']
        self.lang = config['XMLRiver']['Google']['default_language_id']
        self.device = config['XMLRiver']['Google']['default_device']
        self.use_lang = config['XMLRiver']['Google']['default_use_language']

    # Set locale for search
    def set_region(self, locale):
        if locale == 'MOSCOW':
            self.country = 2643
            self.loc = 1001493
        elif locale == 'MINSK':
            self.country = 2112
            self.loc = 1001493
        elif locale == 'KIEV':
            self.country = 2804
            self.loc = 1012852

    # Change location of search
    def set_loc(self, country_id, loc_id):
        self.country = country_id
        self.loc = loc_id

    # Return request to xml river api with keys
    def get_request(self, keys):
        return 'http://xmlriver.com/search/xml?user=' + self.xml_river_user+'&key=' + self.xml_river_key +\
               '&query=' + keys + '&groupby=' + str(self.group_by) + '&loc=' + str(self.loc) + \
               '&country=' + str(self.country) + ('&lr='+self.lang if self.use_lang else '') + '&device=' + self.device


