import flask
import gzip
import json
import jwt
import logging
import os
import queue
import re
import requests
import threading
import time

WD_API_URL = os.getenv('WD_API_URL')
WD_API_KEY = os.getenv('WD_API_KEY')
WEBHOOK_SECRET = os.getenv('WEBHOOK_SECRET')
IBM_CLOUD_API_KEY = os.getenv('IBM_CLOUD_API_KEY')
WML_ENDPOINT_URL = os.getenv('WML_ENDPOINT_URL', 'https://us-south.ml.cloud.ibm.com')
WML_INSTANCE_CRN = os.getenv('WML_INSTANCE_CRN')

# Enrichment task queue
q = queue.Queue()

app = flask.Flask(__name__)
app.logger.setLevel(logging.INFO)
app.logger.handlers[0].setFormatter(logging.Formatter('[%(asctime)s] %(levelname)s in %(module)s: %(message)s (%(filename)s:%(lineno)d)'))

def get_iam_token():
    data = {'grant_type': 'urn:ibm:params:oauth:grant-type:apikey', 'apikey': IBM_CLOUD_API_KEY}
    response = requests.post('https://iam.cloud.ibm.com/identity/token', data=data)
    if response.status_code == 200:
        return response.json()['access_token']
    else:
        raise Exception('Failed to get IAM token.')

IAM_TOKEN = None

def extract_entities(text):
    global IAM_TOKEN
    if IAM_TOKEN is None:
        IAM_TOKEN = get_iam_token()
    # Prompt
    payload = {
        'model_id': 'mistralai/mixtral-8x7b-instruct-v01',
        'input': f'''[INST]ou are a doctor who classifies passages from a patient record in Finnish into one or more of 10 categories based on the International Classification of Functioning, Disability and Health (ICF) codes and labels in Finnish. You must classify using the following child codes of B280:  Kipuaistimus ('Pain'):
B28010: Kipu päässä ja niskassa
B28011: Kipu rinnassa
B28012: Kipu mahassa tai vatsassa
B28013: Kipu selässä
B28014: Kipu yläraajassa
B28015: Kipu alaraajassa
B28016: Kipu nivelissä
B28018: Kipu ruumiin/kehon osassa, muu määritelty
B28019: Kipu ruumiin/kehon osassa, määrittelemätön
B2804: Alueellisesti säteilevä kipu

Follow these rules:
1) Return all the applicable codes and the word, words or phrases that serve as evidence for the code. Always include the word that describes the pain. 
2) If evidence exists for the same code multiple times, return the code multiple times paired with each word or phrase evidence. Return the shortest possible evidence.
3) Do not make up new codes.
4) Re-write the evidence phrase to be more readable, and return only grammatically correct Finnish.
5) Return codes B2810 - B2810 with the name of the body part only if pain in that specific body part is mentioned.
6) Return B28010 only when pain is in head or neck. 
7) Return B28011 only when pain is in the chest. 
8) Return B28012 only when pain is in the stomach.
9) Return B28013 when pain is in the back or spine.
10) Return B28014 when pain is in an upper limb, arm, hand, wrist, elbow, shoulder etc.
11) Return B28015 when pain is in a lower limb, leg, foott, calf, thigh etc.
12) Return B28016 when pain is in a joint.
13) Return code B28018 if pain in multiple body parts are mentioned.
14) Return code B28019 only if pain is mentioned but not the body part.
15) Return code B2804 for radiating pain.
16) If the patient record does not belong to any category, return null.
17) Return the output as a comma separated list of ICF code paired with each words or phrase that serves as evidence of the code. 
18) Do not add a full stop at the end of the output

Examples: 
Input: Tulosyy: Selkäkipu. Anamneesi: 42-vuotias nainen, sairaanhoitaja. Ei tunnettuja perussairauksia. Työskentelee sairaalassa, jossa joutuu usein nostamaan ja kääntämään raskaita potilaita. Kaksi viikkoa sitten nosti painavaa potilasta, jolloin tunsi äkillisen kivun alaselässä. Kipua myös niskassa ja yläraajoissa. Kipu pahenee erityisesti työpäivän jälkeen. Kotona liikkuminen hankalaa kivun vuoksi, erityisesti portaiden nouseminen ja istuma-asennosta nouseminen. Kipua lievittää lepo, mutta kipu palaa nopeasti rasituksen myötä. VAS-asteikolla kipu 6/10. Ei virtsaamisvaikeuksia, ei säteilyoireita, ei puutumista. Status: Yleistila hyvä. Nousee tuolista varovaisesti, kävelee hitaasti ja varovasti. Selän taivutukset rajoittuneet, ilmoittaa kivun keskittyvän alaselkään ja yläraajoihin. Etenkin taivutus eteenpäin provosoi kipua. Alaraajoissa lihasvoimat symmetriset, kyykkyyn ja ylös pääsee vaivoin. Nilkkojen voimat täydet, ei tuntopuutoksia, refleksit normaalit. Suunnitelma: Todennäköisesti lihasperäinen selkäkipu ja ylirasitus. Neuvotaan vastaanotolla venyttely- ja lihasharjoitukset. Kannustetaan liikkumaan mahdollisuuksien mukaan normaalisti. Suositellaan tulehduskipulääkettä, kuten ibuprofeenia tarvittaessa. Lämpöhoitoa voi kokeilla paikallisesti kivun lievittämiseksi. Jos oireet eivät ala helpottaa kahden viikon sisään, suositellaan varaamaan aika fysioterapeutille. Diagnoosi (ICD-10): M54.5 Lanneselän kipu
Output: 
B28013: "selkäkipu", B28013: "alaselkäkipu", B28013: "kipu yltää välillä myös yläselkään", B28013: "kipua ilmoittaa alaselkään", B28013: "selän taivutus eteenpäin ja taaksepäin provosoi kivun", B28013: "lanneselän kipu" 

Input: Tulosyy: Borrelioosin diagnosointi ja jatkuvat nivelkivut. Anamneesi: 14-vuotias poika. Aiemmin epäilty punkin purema ja pyöreä ihomerkki. Kaksi viikkoa sitten otetut verikokeet osoittivat borrelioosin. Nivelkivut jatkuvat samalla tasolla kuin edellisellä käynnillä. Huoli kyvystä osallistua koulun liikuntatunneille on kasvanut. Status: Yleistila hyvä. Nivelkivut edelleen läsnä, erityisesti aamuisin ja iltaisin. Kivut edelleen paikallisia, ei merkkiä hermoperäisestä särystä. Ihomerkki näkyy yhä, mutta ei tulehtunut. Psykologinen stressi näkyy huolena terveydentilasta ja osallistumisesta koulun toimintaan. Suunnitelma: Aloittaa välittömästi antibioottikuuri borrelioosin hoitoon. Seurataan kivun kehittymistä ja vasteita hoitoon. Jatketaan kevennettyä ohjelmaa koululiikunnassa ja tarjotaan psykologista tukea sopeutumisessa koulun sosiaaliseen elämään. Mikäli kivut eivät helpota hoidon aikana, harkitaan jatkotutkimuksia ja konsultaatiota lastenreumatologin kanssa. Diagnoosi (ICD-10): A69.2 Lyme tauti (borrelioosi)
Output: 
B28016: "nivelkivut", B28018: "kivut edelleen paikallisia", B2804: "hermoperäinen särky"


Input: {text}[/INST] 
Output:
''',
        'parameters': {
            'decoding_method': 'sample',
            'max_new_tokens': 250,
            'min_new_tokens': 0,
            'random_seed': 10,
            'stop_sequences': [
   "\n\n"
  ],
            'temperature': 0.5,
            'top_k': 35,
            'top_p': 0.5,
            'repetition_penalty': 1
        },
        'wml_instance_crn': WML_INSTANCE_CRN
    }
    params = {'version': '2023-05-29'}
    headers = {'Authorization': f'Bearer {IAM_TOKEN}'}
    response = requests.post(f'{WML_ENDPOINT_URL}/ml/v1-beta/generation/text', json=payload, params=params, headers=headers)
    if response.status_code == 200:
        result = response.json()['results'][0]['generated_text']
        app.logger.info('LLM result: %s', result)
        entities = []
        if result == 'None':
            # No entity found
            return entities
        for pair in re.split(r',\s*', result):
            text_type = re.split(r':\s*', pair)
            if len(text_type) == 2:
                entities.append({'text': text_type[1], 'type': text_type[0]})
        return entities
    elif response.status_code == 401:
        # Token expired. Re-generate it.
        IAM_TOKEN = get_iam_token()
        return extract_entities(text)
    else:
        raise Exception(f'Failed to generate: {response.text}')

def enrich(doc):
    app.logger.info('doc: %s', doc)
    features_to_send = []
    for feature in doc['features']:
        # Target 'text' field
        if feature['properties']['field_name'] != 'text':
            continue
        location = feature['location']
        begin = location['begin']
        end = location['end']
        text = doc['artifact'][begin:end]
        try:
            # Entity extraction example
            results = extract_entities(text)
            app.logger.info('entities: %s', results)
            for entity in results:
                entity_text = entity['text']
                entity_type = entity['type']
                for matched in re.finditer(re.escape(entity_text), text):
                    features_to_send.append(
                        {
                            'type': 'annotation',
                            'location': {
                                'begin': matched.start() + begin,
                                'end': matched.end() + begin,
                            },
                            'properties': {
                                'type': 'entities',
                                'confidence': 1.0,
                                'entity_type': entity_type,
                                'entity_text': matched.group(0),
                            },
                        }
                    )
        except Exception as e:
            # Notice example
            features_to_send.append(
                {
                    'type': 'notice',
                    'properties': {
                        'description': str(e),
                        'created': round(time.time() * 1000),
                    },
                }
            )
    app.logger.info('features_to_send: %s', features_to_send)
    return {'document_id': doc['document_id'], 'features': features_to_send}

def enrichment_worker():
    while True:
        item = q.get()
        version = item['version']
        data = item['data']
        project_id = data['project_id']
        collection_id = data['collection_id']
        batch_id = data['batch_id']
        batch_api = f'{WD_API_URL}/v2/projects/{project_id}/collections/{collection_id}/batches/{batch_id}'
        params = {'version': version}
        auth = ('apikey', WD_API_KEY)
        headers = {'Accept-Encoding': 'gzip'}
        try:
            # Get documents from WD
            response = requests.get(batch_api, params=params, auth=auth, headers=headers, stream=True)
            status_code = response.status_code
            app.logger.info('Pulled a batch: %s, status: %d', batch_id, status_code)
            if status_code == 200:
                # Annotate documents
                enriched_docs = [enrich(json.loads(line)) for line in response.iter_lines()]
                files = {
                    'file': (
                        'data.ndjson.gz',
                        gzip.compress(
                            '\n'.join(
                                [json.dumps(enriched_doc) for enriched_doc in enriched_docs]
                            ).encode('utf-8')
                        ),
                        'application/x-ndjson'
                    )
                }
                # Upload annotated documents
                response = requests.post(batch_api, params=params, files=files, auth=auth)
                status_code = response.status_code
                app.logger.info('Pushed a batch: %s, status: %d', batch_id, status_code)
        except Exception as e:
            app.logger.error('An error occurred: %s', e, exc_info=True)
            # Retry
            q.put(item)

# Turn on the enrichment worker thread
threading.Thread(target=enrichment_worker, daemon=True).start()

# Webhook endpoint
@app.route('/webhook', methods=['POST'])
def webhook():
    # Verify JWT token
    header = flask.request.headers.get('Authorization')
    _, token = header.split()
    try:
        jwt.decode(token, WEBHOOK_SECRET, algorithms=['HS256'])
    except jwt.PyJWTError as e:
        app.logger.error('Invalid token: %s', e)
        return {'status': 'unauthorized'}, 401
    # Process webhook event
    data = flask.json.loads(flask.request.data)
    app.logger.info('Received event: %s', data)
    event = data['event']
    if event == 'ping':
        # Receive this event when a webhook enrichment is created
        code = 200
        status = 'ok'
    elif event == 'enrichment.batch.created':
        # Receive this event when a batch of the documents gets ready
        code = 202
        status = 'accepted'
        # Put an enrichment request into the queue
        q.put(data)
    else:
        # Unknown event type
        code = 400
        status = 'bad request'
    return {'status': status}, code

PORT = os.getenv('PORT', '8080')
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(PORT))
