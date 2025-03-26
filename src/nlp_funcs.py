import spacy
nlp = spacy.load('en_core_web_sm')

def find_persons(text):
    # Create Doc object
    doc2 = nlp(text)

    # Identify the persons
    persons = [ent.text for ent in doc2.ents if ent.label_ == 'PERSON']

    # Return persons
    return persons

# fuzzy matching - won't work without building corpus
def match_string(input_doc, final_list):
    # Calculate similarity
    similarities = [(item, nlp(item).similarity(nlp(input_doc))) for item in final_list]
    best_match = max(similarities, key=lambda item: item[1])
    

    return best_match