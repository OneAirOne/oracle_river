Oracle_river - transfert de donnees depuis oracle vers elasticsearch en SQL incrémental
=======================================================================================

Ce module permet de recuperer des requetes SQL en mode incremental dans une base de donnees Oracle et de les stocker
sous format JSON dans elasticsearch.
L incrémental des données sera gere via une base MySQL (date de derniere execution)

Vous pouvez l'installer avec pip:

    pip install oracle_river	

si vous rencontrez un message d erreur c est probablement que vous n avez pas le module pyelasticsearch 
qui sert de dépendance à oracle_river.
Installez le et reitérez la commande pip install oracle_river


    |Oracle|  => SQL incremental + serialisation JSON => | elasticsearch |
 

Exemple d'usage:

    1) CREER UNE BASE MYSQL
        creez une base au nom de votre choix
        creez une table histo_connexion avec le format ci-dessous:
	ID_CONNEXION / NOM_BASE / NOM_SCHEMA / DTM_DERNIERE_EXEC
        autoincrement/string 256/string256  /Timestamp						


    2) REALISER LES IMPORTS :

	creez un fichier python .py dans lequel vous inscrivez les lignes de codes suivantes :
	(vous devez au préalable installer les API de connexion cx_Oralce et mysql.connector)

        #!/usr/bin/env python
        # coding: utf-8

        from datetime import datetime
        import cx_Oracle
        import mysql.connector
        from  pyelasticsearch  import ElasticSearch
        from oracle_river import Load_Data

    3) DEFINIR LES PARAMETRES DE CONNEXION:

        # ouverture de la connexion à la base oracle
        DB = 'pythonhol/welcome@127.0.0.1/orcl' (voir http://www.oracle.com/technetwork/articles/dsl/python-091105.html)
        OracleCnx = cx_Oracle.connect(DB)
        # paramètres de connexion elasticseach
        es = ElasticSearch('http://localhost:9200/')
        # ouverture de la connexion MySQL
        MySQLCnx = mysql.connector.connect(user='root', password='', host='localhost', database='elasticsearch_river')

    4) PREPARER LA REQUETE:

	renseignez les paramètre ci-dessous :

        NomTable = 'CUSTOMER' (pour la base d'administration MySql)
        DtmInit = '2013-09-01 00:00:00' (si vous executez la requete pour la première fois)
        DocType = 'CUSTOMER_ID' (clé primaire de la table oracle utilisé comme type d index sur elasticsearch)
        Index = 'dbcustomer'  (pour elasticsearch, minuscules obligatoire par elasticsearch)
        NomBase = 'DBBUSINESS' (pour la base d'administration MySql)
        NomSchema = 'ACTIVITY' (pour la base d'administration MySql)
        Query = ("SELECT * FROM ACTIVITY.CUSTOMER WHERE CREATION_DTM >= to_timestamp('\
                    {0}', 'YYYY-MM-DD HH24:MI:SS') OR LAST_UPDATE_DTM>= to_timestamp('\
                    {0}', 'YYYY-MM-DD HH24:MI:SS')")# les paramétres s'écrivent comme cela : "{0},{1},{n}".format(valeur0, valeur1, valeurn)

    5) EXECUTER LA REQUETE VIA LA FONCTION PRESENTE DANS LE MODULE:

        Load_Data(MySQLCnx, es, OracleCnx, Query, DtmInit, DocType, Index, NomBase, NomSchema, NomTable)

	la requete defini plus haut va s executer et les donnees vont etre transferees a elasticseearch
	par morceaux via le protocol http 

Ce code est sous licence WTFPL.