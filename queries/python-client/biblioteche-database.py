"""
    Database creato con il client Python di TerminusDB.
    Fonte dei dati: http://dati.regione.sardegna.it/dataset/biblioteche-della-sardegna
"""

# Import moduli terminus-db:
from terminusdb_client import WOQLQuery
from terminusdb_client import WOQLClient
from terminusdb_client import woqldataframe

# Definizioni:
server_url = "https://127.0.0.1:6363"
user = "admin"
account = "admin"
key = "root"
dbid = "biblioteche-db"
repository = "local"
label = "Biblioteche della Sardegna Database"
description = "Database delle biblioteche della Sardegna creato attraverso il client Python."

# Connessione al server mediante client:
client = WOQLClient(server_url)
result = client.connect(user=user,account=account,key=key,db=dbid, insecure=True) 


# Creazione database (se è già esistente, viene eliminato e ricreato):
try: 
    client.create_database(dbid,user,label=label, description=description)
except Exception as E:
    error_obj = E.errorObj
    if "api:DatabaseAlreadyExists" == error_obj.get("api:error",{}).get("@type",None):
        print(f'Warning: Database "{dbid}" already exists! Now it will be deleted and created again.\n')
        client.delete_database(dbid) 
        client.create_database(dbid,user,label=label, description=description)
    else:
        raise(E)


# Creazione schema del database:
def create_schema(client, commit_msg):
    schema = WOQLQuery().woql_and(
        WOQLQuery().doctype("Biblioteca", 
                            label="Biblioteca",
                            description="Documento che rappresenta una biblioteca della Sardegna").
                            property("isil", "string", label="Codice ISIL").
                            property("tipologia_funzionale", "string", label="Tipologia funzionale").
                            property("tipologia_amministrativa", "string", label="Tipologia amministrativa").
                            property("sbn", "string", label="Codice SBN").
                            property("indirizzo", "string", label="Indirizzo").
                            property("cap", "string", label="CAP").
                            property("comune", "string", label="Comune").
                            property("provincia", "string", label="Provincia").
                            property("telefono", "string", label="Telefono").
                            property("fax", "string", label="Fax").
                            property("email", "string", label="Email").
                            property("long", "string", label="Longitudine"). 
                            property("lat", "string", label="Latitudine") 
    )
    
    return schema.execute(client, commit_msg)


# Estrae i dati dal CSV e li memorizza nelle variabili (v:)
def get_csv_variables(path):
    csv = WOQLQuery().get(
        WOQLQuery().
        woql_as("CODICE_ISIL", "v:CodiceIsil").
        woql_as("DENOMINAZIONE", "v:Denominazione").
        woql_as("TIPOLOGIA_FUNZIONALE", "v:TipFunzionale").
        woql_as("TIPOLOGIA_AMMINISTRATIVA", "v:TipAmministrativa").
        woql_as("CODICE_SBN", "v:CodiceSBN").
        woql_as("INDIRIZZO", "v:Indirizzo").
        woql_as("CAP", "v:Cap").
        woql_as("COMUNE", "v:Comune").
        woql_as("PROVINCIA", "v:Provincia").
        woql_as("TELEFONO", "v:Telefono").
        woql_as("FAX", "v:Fax").
        woql_as("EMAIL", "v:Email").
        woql_as("LONGITUDINE", "v:Longitudine").
        woql_as("LATITUDINE", "v:Latitudine")
    ).file(path) #remote dà errore (SSL) se chiamato su http invece che https
    
    return csv


# Inserisce le variabili create col metodo get_csv_variables() nel database:
def get_inserts():
    inserts = WOQLQuery().woql_and( 
        WOQLQuery().insert("v:BibliotecaID", "Biblioteca", label="v:Denominazione"). 
            property("isil", "v:CodiceIsil"). # ("nomeNelDatabase", "v:Variabile")
            property("tipologia_funzionale", "v:TipFunzionale").
            property("tipologia_amministrativa", "v:TipAmministrativa").
            property("sbn", "v:CodiceSBN").
            property("indirizzo", "v:Indirizzo").
            property("cap", "v:Cap").
            property("comune", "v:Comune").
            property("provincia", "v:Provincia").
            property("telefono", "v:Telefono").
            property("fax", "v:Fax").
            property("email", "v:Email").
            property("long", "v:Longitudine").
            property("lat", "v:Latitudine")
    )
    return inserts


# Inserisce nel database le istanze del file CSV con un id generato
def populate_db(client, commit_msg, csv):
    wrangle = WOQLQuery().idgen("doc:Biblioteca", ["v:CodiceIsil", "v:Comune"], "v:BibliotecaID")
    inputs = WOQLQuery().woql_and(csv, wrangle)
    answer = WOQLQuery().when(inputs, get_inserts())
    answer.execute(client, commit_msg)


# Tutte le biblioteche di Sassari e, se esistono, il loro codice SBM e la loro mail:
def biblioteche_sassari(client):
    conditions = [
        WOQLQuery().triple("v:Biblioteca", "type", "scm:Biblioteca"),
        WOQLQuery().triple("v:Biblioteca", "label", "v:Name"), 
        WOQLQuery().opt().triple("v:Biblioteca", "sbn", "v:CodiceSBN"),
        WOQLQuery().triple("v:Biblioteca", "comune", "v:Comune"),
        WOQLQuery().triple("v:Biblioteca", "comune", "Sassari"),
        WOQLQuery().opt().triple("v:Biblioteca", "email", "v:Email")]

    query = WOQLQuery().select("v:Name", "v:CodiceSBN", "v:Comune", "v:Email").woql_and(*conditions)
    return query.execute(client)


# Tutte le biblioteche che non hanno un'email:
def no_email_biblioteche(client):
    conditions = [
        WOQLQuery().triple("v:Biblioteca", "type", "scm:Biblioteca"),
        WOQLQuery().triple("v:Biblioteca", "label", "v:Name"), 
        WOQLQuery().triple("v:Biblioteca", "email", "v:Email"),
        WOQLQuery().triple("v:Biblioteca", "email", "")
        ]

    query = WOQLQuery().select("v:Name", "v:Email").woql_and(*conditions)
    return query.execute(client)

# Esecuzione query:
create_schema(client, "Creazione schema (Biblioteca)")

url = "http://intranet.sardegnabiblioteche.it/opendata/anagrafica_biblioteche.csv"
file_path = "/home/strausste/TerminusDB/csv/anagrafica_biblioteche.csv"

populate_db(client, "Popolo il database", get_csv_variables(file_path))

# Select:
result = no_email_biblioteche(client)
woqldataframe.result_to_df(result)