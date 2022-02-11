def get_url_level(url:str):
    return url.count('/')

def get_url_parent(url:str):
    return url.rsplit('/', 1)[0]

def get_url_part(url, level):
    return '/'.join(url.split('/')[:level + 1]) # .replace('/vacancies', '')

def get_url_body(url):
    return '/'.join(url.split('/')[3:])

def get_url_end(url):
    return url.split('/', 1)[1]