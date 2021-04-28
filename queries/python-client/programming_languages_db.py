"""
    A TerminusDB database implemented in Python.
    This database consists in programming languages and softwares written using these programming languages.
    @Author: strausste
"""

# Importing the terminusdb client modules:
from terminusdb_client import WOQLQuery
from terminusdb_client import WOQLClient
from terminusdb_client import woqldataframe

# Definitions:
server_url = "https://127.0.0.1:6363"
user = "admin"
account = "admin"
key = "root"
dbid = "programming-languages-db"
repository = "local"
label = "Programming Languages Database"
description = "This database was created in Python using the terminusdb-client."

# Connecting to the database using the client:
client = WOQLClient(server_url)
result = client.connect(user=user,account=account,key=key,db=dbid, insecure=True) 


# Creating the database (if this already exists, it will be created again):
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


# Query which defines the database's schema:
def create_schema(client, commit_msg):
    schema = WOQLQuery().woql_and(
        WOQLQuery().doctype("Language",
                            label="Programming Language",
                            description="An existing programming language").
                            property("paradigm", "string", label="Paradigm").   # this property might be a doctype as well in the future
                            property("file_extension", "string", label="File Extension").
                            property("developer", "string", label="Developer"). # this property might be a doctype as well in the future
                            property("influenced_by", "Language", label="Influenced By"),
        WOQLQuery().doctype("Software", 
                            label="Software",
                            description="A computer program written in a programming language").
                            property("language", "Language", label="Written In").
                            property("lines_of_code", "integer", label="Lines of Code").
                            property("author", "string", label="Author")        # this property might be a doctype as well in the future
    )

    return schema.execute(client, commit_msg)


# This query populates the database:
def populate_db(client, commit_msg):
    insert = WOQLQuery().woql_and(

        # Programming languages:
        WOQLQuery().insert("c", "Language", label = "C").
        property("paradigm", "Imperative").
        property("file_extension", ".c").
        property("developer", "Dennis Ritchie"),
        WOQLQuery().insert("java", "Language", label = "Java").
        property("paradigm", "Object Oriented").
        property("file_extension", ".java").
        property("developer", "Oracle Corporation").
        property("influenced_by", "doc:c"),
        WOQLQuery().insert("cpp", "Language", label = "C++").
        property("paradigm", "Object Oriented").
        property("file_extension", ".cpp").
        property("developer", "Bjarne Stroustrup").
        property("influenced_by", "doc:c"),
        WOQLQuery().insert("cs", "Language", label = "C#").
        property("paradigm", "Object Oriented").
        property("file_extension", ".cs").
        property("developer", "Microsoft").
        property("influenced_by", "doc:cpp"),
        WOQLQuery().insert("javascript", "Language", label = "JavaScript").
        property("paradigm", "Event Oriented").
        property("file_extension", ".js").
        property("developer", "Brendan Eich").
        property("influenced_by", "doc:c"),
        WOQLQuery().insert("python", "Language", label = "Python").
        property("paradigm", "Object Oriented").
        property("file_extension", ".py").
        property("developer", "Guido van Rossum").
        property("influenced_by", "doc:cpp"),     
        WOQLQuery().insert("lisp", "Language", label = "Lisp").
        property("paradigm", "Functional").
        property("file_extension", ".lisp").
        property("developer", "John McCarthy"),
        WOQLQuery().insert("ruby", "Language", label = "Ruby").
        property("paradigm", "Object Oriented").
        property("file_extension", ".rb").
        property("developer", "Yukihiro Matsumoto").
        property("influenced_by", "doc:lisp"),
        WOQLQuery().insert("haskel", "Language", label = "Haskel").
        property("paradigm", "Functional").
        property("file_extension", ".hs").
        property("developer", "Simon Peyton Jones").
        property("influenced_by", "doc:lisp"),
        WOQLQuery().insert("ocaml", "Language", label = "OCaml").
        property("paradigm", "Functional").
        property("file_extension", ".ml").
        property("developer", "INRIA"),
        WOQLQuery().insert("smalltalk", "Language", label = "Smalltalk").
        property("paradigm", "Object Oriented").
        property("file_extension", ".st").
        property("developer", "Alan Kay").
        property("influenced_by", "doc:lisp"),
        WOQLQuery().insert("objective-c", "Language", label = "Objective-C").
        property("paradigm", "Object Oriented").
        property("file_extension", ".h").
        property("developer", "Brad Cox").
        property("influenced_by", "doc:smalltalk"),

        # Softwares:
        WOQLQuery().insert("linux", "Software", label = "Linux (kernel)").
        property("language", "doc:c").
        property("lines_of_code", 27800000).
        property("author", "Linus Torvalds"),
        WOQLQuery().insert("idtech3", "Software", label = "id Tech 3").
        property("language", "doc:c").
        property("lines_of_code", 400000).
        property("author", "id Software"),
        WOQLQuery().insert("firefox", "Software", label = "Mozilla Firefox").
        property("language", "doc:cpp").
        property("lines_of_code", 21000000).
        property("author", "Mozilla Foundation"),
        WOQLQuery().insert("android", "Software", label = "Android OS").
        property("language", "doc:c").
        property("lines_of_code", 12000000).
        property("author", "Google"),
        WOQLQuery().insert("cryengine2", "Software", label = "CryENGINE 2").
        property("language", "doc:cpp").
        property("lines_of_code", 1000000).
        property("author", "Crytek"),
        WOQLQuery().insert("ue3", "Software", label = "Unreal Engine 3").
        property("language", "doc:cpp").
        property("lines_of_code", 2000000).
        property("author", "Epic Games"),
        WOQLQuery().insert("minecraft", "Software", label = "Minecraft").
        property("language", "doc:java").
        property("lines_of_code", 500000).
        property("author", "Mojang"),
        WOQLQuery().insert("github", "Software", label = "GitHub (Server)").
        property("language", "doc:ruby").
        property("lines_of_code", 250000).
        property("author", "GitHub Inc."),
        WOQLQuery().insert("airbnb", "Software", label = "Airbnb (Server)").
        property("language", "doc:ruby").
        property("lines_of_code", 800000).
        property("author", "Brian Chesky"),
        WOQLQuery().insert("youtube", "Software", label = "YouTube (Frontend)").
        property("language", "doc:javascript").
        property("lines_of_code", 210000).
        property("author", "Google"),
        WOQLQuery().insert("curiosity", "Software", label = "Mars Curiosity Rover (Module Testing)").
        property("language", "doc:python").
        property("lines_of_code", 400000).
        property("author", "JPL"),
        WOQLQuery().insert("openoffice", "Software", label = "Apache Open Office").
        property("language", "doc:java").
        property("lines_of_code", 25000000).
        property("author", "Apache Software Foundation")                                  
    )
    return insert.execute(client, commit_msg)


# Select: (all the softwares written in C++):
def cpp_softwares_query(client):
    conditions = [WOQLQuery().triple("v:Software", "type", "scm:Software"),     # Document (query's domain)
                  WOQLQuery().triple("v:Software", "label", "v:Name"),          # property label saved in WOQL variable (v:Variable)
                  WOQLQuery().triple("v:Software", "author", "v:Author"),
                  WOQLQuery().triple("v:Software", "lines_of_code", "v:Lines"),
                  WOQLQuery().triple("v:Software", "language", "v:Language"),
                  WOQLQuery().triple("v:Software", "language", "doc:cpp")]      # Condition: "language" = "C++"

    query = WOQLQuery().select("v:Name","v:Author", "v:Lines").woql_and(*conditions) # Output variables
    return query.execute(client)


# Select: (all the object oriented languages):
def oo_languages_query(client):
    conditions = [WOQLQuery().triple("v:Language", "type", "scm:Language"), 
                  WOQLQuery().triple("v:Language", "label", "v:Name"),  
                  WOQLQuery().triple("v:Language", "paradigm", "v:Paradigm"),      
                  WOQLQuery().triple("v:Language", "file_extension", "v:FileExt"),
                  WOQLQuery().triple("v:Language", "developer", "v:Developer"),
                  WOQLQuery().triple("v:Language", "paradigm", "Object Oriented")]

    query = WOQLQuery().select("v:Paradigm","v:FileExt", "v:Developer", "v:Name").woql_and(*conditions)
    return query.execute(client)


# Select: (all the softwares written in C++ or in Java):
def cpp_or_java_softwares_query(client):
    conditions = [WOQLQuery().triple("v:Software", "type", "scm:Software"), 
                  WOQLQuery().triple("v:Software", "label", "v:Name"),      
                  WOQLQuery().triple("v:Software", "author", "v:Author"),
                  WOQLQuery().triple("v:Software", "lines_of_code", "v:Lines"),
                  WOQLQuery().triple("v:Software", "language", "v:Language"),
                  WOQLQuery().woql_or(
                      WOQLQuery().triple("v:Software", "language", "doc:cpp"),
                      WOQLQuery().triple("v:Software", "language", "doc:java")
                      )]

    query = WOQLQuery().select("v:Name","v:Author", "v:Lines").woql_and(*conditions)
    return query.execute(client)


# Select: (all the softwares with more than 10 million lines of code):
def certain_loc_software_query(client):
    conditions = [WOQLQuery().triple("v:Software", "type", "scm:Software"), 
                  WOQLQuery().triple("v:Software", "label", "v:Name"),      
                  WOQLQuery().triple("v:Software", "author", "v:Author"),
                  WOQLQuery().triple("v:Software", "lines_of_code", "v:Lines"),
                  WOQLQuery().triple("v:Software", "language", "v:Language")]

    query = WOQLQuery().select("v:Name","v:Author", "v:Lines").woql_and(*conditions).greater("v:Lines", 10000000)
    return query.execute(client)


# Select: (the language which indirectly influenced C#):
def language_influenced_by(client):
    conditions = [WOQLQuery().triple("v:Language", "type", "scm:Language"), 
                  WOQLQuery().triple("v:Language", "label", "v:Name"),  
                  WOQLQuery().triple("v:Language", "paradigm", "v:Paradigm"),      
                  WOQLQuery().triple("v:Language", "file_extension", "v:FileExt"),
                  WOQLQuery().triple("v:Language", "developer", "v:Developer"),
                  WOQLQuery().triple("v:Language", "label", "C#"),                  # select only the C# language
                  WOQLQuery().triple("v:Language", "influenced_by", "v:DirInf"),    # select the property influenced_by 
                  WOQLQuery().triple("v:DirInf", "label", "v:DirInfName"),          # the name of that language
                  WOQLQuery().triple("v:DirInf", "influenced_by", "v:UndInf"),      # select the language which directly influenced the language which directly influenced C# (so the language which INDIRECTLY influenced C#)
                  WOQLQuery().triple("v:UndInf", "label", "v:UndInfName"),          # the name of that language
                  ] 

    query = WOQLQuery().select("v:Paradigm","v:FileExt", "v:Developer", "v:Name", "v:DirInfName", "v:UndInfName").woql_and(*conditions)
    return query.execute(client)


# Query execution:
create_schema(client, "Created the schema (programming languages and softwares).")
populate_db(client, "Populated the database.)

# Calling some of the select methods defined above:
result = cpp_softwares_query(client)
woqldataframe.result_to_df(result)

result = oo_languages_query(client)
woqldataframe.result_to_df(result)

result = cpp_or_java_softwares_query(client)
woqldataframe.result_to_df(result)

result = language_influenced_by(client)
woqldataframe.result_to_df(result)

# Delete example:
WOQLQuery().delete_object("doc:haskel").execute(client, "Deleted the Haskel document.")

# Update example (input value):
WOQLQuery().woql_and(
    WOQLQuery().triple("doc:linux", "scm:lines_of_code", "v:Lines"),
    WOQLQuery().delete_triple("doc:linux", "scm:lines_of_code", "v:Lines"),
    WOQLQuery().add_triple("doc:linux", "scm:lines_of_code", 28000000),
).execute(client, "Updated the Linux kernel's lines of code (manually).")

# Update example (sum evaluation):
WOQLQuery().woql_and(
    WOQLQuery().triple("doc:linux", "scm:lines_of_code", "v:Lines"),
    WOQLQuery().delete_triple("doc:linux", "scm:lines_of_code", "v:Lines"),
    WOQLQuery().eval(WOQLQuery().plus("v:Lines", 260), "v:NewLines"),
    WOQLQuery().add_triple("doc:linux", "scm:lines_of_code", "v:NewLines")
).execute(client, "Update: now Linux kernel has 260 more lines of code.")
