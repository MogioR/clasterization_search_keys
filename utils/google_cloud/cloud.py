from google.cloud import language_v1
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]='credentials.json'

# https://cloud.google.com/natural-language/docs/analyzing-syntax?hl=ru документация по методам
def analyze_entities(text_content):
    """
    Analyzing Entities in text file stored in Cloud Storage

    Args:
      gcs_content_uri Google Cloud Storage URI where the file content is located.
      e.g. gs://[Your Bucket]/[Path to File]
    """
    
    client = language_v1.LanguageServiceClient()

    # gcs_content_uri = 'gs://cloud-samples-data/language/entity.txt'

    # Available types: PLAIN_TEXT, HTML
    type_ = language_v1.Document.Type.PLAIN_TEXT

    # Optional. If not specified, the language is automatically detected.
    # For list of supported languages:
    # https://cloud.google.com/natural-language/docs/languages
    language = "ru"
    document = {"content": text_content, "type_": type_, "language": language}

    # Available values: NONE, UTF8, UTF16, UTF32
    encoding_type = language_v1.Document.Type.PLAIN_TEXT

    response = client.analyze_entities(request = {'document': document, 'encoding_type': encoding_type})
    # Loop through entitites returned from the API
    result = list()
    for entity in response.entities:
        result_dict = dict()
        result.append(result_dict)
        result_dict.update({
            'name':entity.name,
            #'count':entity._pb.NAME_FIELD_NUMBER,
            'score':round(entity.salience, 3),
            'type':language_v1.Entity.Type(entity.type_).name,
        })
        '''
        print(u"Representative name for the entity: {}".format(entity.name))
        # Get entity type, e.g. PERSON, LOCATION, ADDRESS, NUMBER, et al
        print(u"Entity type: {}".format(language_v1.Entity.Type(entity.type_).name))
        # Get the salience score associated with the entity in the [0, 1.0] range
        print(u"Salience score: {}".format(entity.salience))
        # Loop over the metadata associated with entity. For many known entities,
        # the metadata is a Wikipedia URL (wikipedia_url) and Knowledge Graph MID (mid).
        # Some entity types may have additional metadata, e.g. ADDRESS entities
        # may have metadata for the address street_name, postal_code, et al.
        for metadata_name, metadata_value in entity.metadata.items():
            print(u"{}: {}".format(metadata_name, metadata_value))

        # Loop over the mentions of this entity in the input document.
        # The API currently supports proper noun mentions.
        for mention in entity.mentions:
            print(u"Mention text: {}".format(mention.text.content))
            # Get the mention type, e.g. PROPER for proper noun
            print(
                u"Mention type: {}".format(language_v1.EntityMention.Type(mention.type_).name)
            )

    # Get the language of the text, which will be the same as
    # the language specified in the request or, if not specified,
    # the automatically-detected language.
    print(u"Language of the text: {}".format(response.language))
    '''
    return result   

def analyze_syntax(text_content):
    """
    Analyzing Syntax in a String

    Args:
      text_content The text content to analyze
    """

    client = language_v1.LanguageServiceClient()
    # text_content = 'This is a short sentence.'

    # Available types: PLAIN_TEXT, HTML
    type_ = language_v1.Document.Type.PLAIN_TEXT

    # Optional. If not specified, the language is automatically detected.
    # For list of supported languages:
    # https://cloud.google.com/natural-language/docs/languages
    language = "ru"
    document = {"content": text_content, "type_": type_, "language": language}

    # Available values: NONE, UTF8, UTF16, UTF32
    encoding_type = language_v1.EncodingType.UTF8

    response = client.analyze_syntax(request = {'document': document, 'encoding_type': encoding_type})
    # Loop through tokens returned from the API
    for token in response.tokens:
        # Get the text content of this token. Usually a word or punctuation.
        text = token.text
        print(u"Token text: {}".format(text.content))
        print(
            u"Location of this token in overall document: {}".format(text.begin_offset)
        )
        # Get the part of speech information for this token.
        # Parts of spech are as defined in:
        # http://www.lrec-conf.org/proceedings/lrec2012/pdf/274_Paper.pdf
        part_of_speech = token.part_of_speech
        # Get the tag, e.g. NOUN, ADJ for Adjective, et al.
        print(
            u"Part of Speech tag: {}".format(
                language_v1.PartOfSpeech.Tag(part_of_speech.tag).name
            )
        )
        # Get the voice, e.g. ACTIVE or PASSIVE
        print(u"Voice: {}".format(language_v1.PartOfSpeech.Voice(part_of_speech.voice).name))
        # Get the tense, e.g. PAST, FUTURE, PRESENT, et al.
        print(u"Tense: {}".format(language_v1.PartOfSpeech.Tense(part_of_speech.tense).name))
        # See API reference for additional Part of Speech information available
        # Get the lemma of the token. Wikipedia lemma description
        # https://en.wikipedia.org/wiki/Lemma_(morphology)
        print(u"Lemma: {}".format(token.lemma))
        # Get the dependency tree parse information for this token.
        # For more information on dependency labels:
        # http://www.aclweb.org/anthology/P13-2017
        dependency_edge = token.dependency_edge
        print(u"Head token index: {}".format(dependency_edge.head_token_index))
        print(
            u"Label: {}".format(language_v1.DependencyEdge.Label(dependency_edge.label).name)
        )

    # Get the language of the text, which will be the same as
    # the language specified in the request or, if not specified,
    # the automatically-detected language.
    print(u"Language of the text: {}".format(response.language))



def google_nlp_entities(text_content,  result_type="all", limit=10, invalid_types = ['NUMBER', 'DATE']):
    def get_type(type):
        return client.enums.Entity.Type(d.type).name
    """
    Analyzing Syntax in a String

    Args:
      text_content The text content to analyze
    """

    client = language_v1.LanguageServiceClient()

    # text_content = 'This is a short sentence.'

    # Available types: PLAIN_TEXT, HTML
    type_ = language_v1.Document.Type.PLAIN_TEXT
    language = "ru"

    document = {"content": text_content, "type_": type_, "language": language}
    features = {'extract_entities': True} #, 'extract_syntax':True
    
    try:
        response = client.annotate_text(document=document, features=features, timeout=20)
    except Exception as e:
        print('Error with language API: ', str(e))
        return []
    
    used = []
    results = []
    #for d in response.sentences:
    #    print(d.text)
    #for d in response.tokens:
    #    print(d.text, d.lemma)
    for d in response.entities:
        
        if limit and len(results) >= limit:
            break
        
        if language_v1.Entity.Type(d.type_).name not in invalid_types and d.name not in used:
            
            data = {'name':d.name,
                       'type':language_v1.Entity.Type(d.type_).name,
                       'salience':d.salience}
            results.append(data)
                
            used.append(d.name)
    return results

if __name__ == '__main__':
    print(google_nlp_entities('Мастером очень довольна. При необходимости буду заказывать именно этого специалиста на вечерний макияж и прическу. Профессионал своего дела. Спасибо.'))
    