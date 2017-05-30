#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    Oracle_river - transfert de donnees depuis oracle vers elasticsearch en SQL incrémental
=======================================================================================

Ce module permet de recuperer des requetes SQL en mode incremental dans une base de donnees Oracle et de les stocker
sous format JSON dans elasticsearch.
L incremental des données sera gere via une base MySQL (date de derniere execution)
Les données vont etre envoyer par morceaux a elastic pour ne pas saturer la memoire
et ne pas se faire deconnecter lors du transfert. 


reportez vous a l adresse ci-dessous pour les informations d utilisation:
https://github.com/OneAirOne/oracle_river

"""
from __future__ import generators
__version__ = "1.0.2"


from datetime import date, datetime, timedelta
from pyelasticsearch import bulk_chunks
import os
os.environ["NLS_LANG"] = ".UTF8"
# problème encodage ascii pour l'analyse de la mémoire
import sys
reload(sys)
sys.setdefaultencoding('utf8')
# from memory_profiler import profile

# variable permetant de limiter l'import
__all__ = ['Load_Data']


#@profile
def Insertion_DTM_Exec(cnx, NomBase, NomSchema, NomTable, date_heure):
    """
    fonction permetant d'insérer la date de dernière execution dans une base d'administration MySQL

    :param cnx: objet de connexion à la base MySQL
    :param NomBase: Nom de la base Oracle  inscrire la base d'adminisatration MySQL
    :param NomSchema: Nom du shéma Oracle
    :param NomTable: Nom de la table Oracle
    :param date_heure: date/heure du debut d'execution de la requete dans oracle
    """
    cursor = cnx.cursor()


    #print cursor

    # insertion de la date de denière execution
    add_connexion = ("INSERT INTO histo_connexion "
                     "(ID_CONNEXION, NOM_BASE, NOM_SCHEMA, NOM_TABLE, DTM_DERNIERE_EXEC) "
                     "VALUES (%(ID)s, %(NOM_BASE)s, %(NOM_SCHEMA)s, %(NOM_TABLE)s, %(DTM_DERNIERE_EXEC)s)")
    # valeurs

    data_connexion = {
        'ID': '',
        'NOM_BASE': NomBase,
        'NOM_SCHEMA': NomSchema,
        'NOM_TABLE': NomTable,
        'DTM_DERNIERE_EXEC': date_heure,
    }

    # ajout d'un nouvelle entrée
    cursor.execute(add_connexion, data_connexion)
    emp_no = cursor.lastrowid

    print emp_no

    # supression du curseur mysql + fermeture de la connexion
    cnx.commit()
    cursor.close()

#@profile
def Recup_DTM_Derniere_Exec(cnx, NomTable):
    """
    fonction permetant de récupérer la date de dernière execution d'une requete dans la base d'administration MySQL

    :param cnx: objet de connexion à la base MySQL
    :param NomBase: Nom de la base Oracle  inscrire la base d'adminisatration MySQL
    :param NomTable: Nom de la table Oracle
    :return date de dernière exécution '2016-03-22 20:17:22.464000'
    """



    cursor = cnx.cursor()
    #print cursor
    #print " => récupération de la dernière date d'éxécution du chargemnt de la table ", NomTable
    #query = "SELECT * FROM histo_connexion "
    #print query

    # récupération de la date de denière execution
    #cursor.execute(query)
    cursor.execute("SELECT max(DTM_DERNIERE_EXEC) FROM histo_connexion  WHERE NOM_TABLE='%s' " % (NomTable))




    result = cursor.fetchall()
    # convertion du résultat en string de date
    result_convert = list(result[0])
    result_convert_date = result_convert[0]
    return result_convert_date

    #cnx.commit()
    cursor.close()

#@profile
def Load_Oracle_to_Elasticsearch(cursor, SQL, es, Cle_primaire, DocType, Index, MySQLCnx, NomBase, NomSchema, NomTable):
    """
    fonction permetant d'executer une requête SQL, de l'encoder en JSON, et de l'indexer dans une base Elasticsearch

    :param cursor: curseur oracle
    :param SQL: requete SQL à envoyer à oracle
    :param es: objet de connexion à Elasticsearch
    :param Cle_primaire: Id de stockage dans elasticsearch
    :param DocType: type de document dans elasticsearch
    :param Index: nom de l'index dans Elasticsearch
    :param MySQLCnx: Objet de connexion à la base d'administration MySQL
    :param NomBase: Nom de la base requeté
    :param NomSchema: Nom du shema requeté
    :param NomTable: Nom de la table requeté
    """

    # date/heure du début de l'execution de la requete Oracle
    req_time = datetime.now()

    debut_requete =datetime.now()
    print ">>> Execution de la requète : %s" % (debut_requete)
    # exection de la requete
    cursor.execute(SQL)
    fin_requete = datetime.now()
    print "    -> durée du traitement : %s" % (fin_requete - debut_requete)

    # récupération des entêtes de colones
    colums = [x[0] for x in cursor.description]

    morceau = 1
    while True:

        debut_fetch = datetime.now()
        print ">>> Début du fetch : %s" % (debut_fetch)
        results = cursor.fetchmany(500000)
        fin_fetch = datetime.now()
        #print ">>> Fin du fetch : %d" % (fin_fetch)
        print "    -> durée du traitement : %s" % (fin_fetch - debut_fetch)

        debut_morceau = datetime.now()
        print ">>> Traitement du morceau %d " % (morceau)

        if not results:
            break
        morceau += 1

        # lecture des résultat et encodage en JSON
        i = 0
        list_dico = []

        for rows in results:
            dico = {colums[colums.index(x)] : rows[colums.index(x)] for x in colums}
            list_dico.append(dico)
            i += 1
            # print i

        # insertion en masse dans elasticsearch
        container = ((es.index_op(doc, id=doc[Cle_primaire]) for doc in list_dico))

        for chunk in bulk_chunks(container, docs_per_chunk= 10000, bytes_per_chunk= 10 * 1024 * 1024): # 10MB taille du morceau
            es.bulk(chunk,doc_type=DocType, index=Index)

        fin_morceau = datetime.now()
        print "    -> durée du traitement : %s" % (fin_morceau - debut_morceau)

    # ecriture de la date de dernière exécution dans MySQL
    Insertion_DTM_Exec(MySQLCnx, NomBase, NomSchema, NomTable, req_time)

    # supression du curseur oracle + fermeture de la connexion
    cursor.close()

#@profile
def Load_Data(MySQLCnx, es, OracleCnx, Query, DtmInit, DocType, Index, NomBase, NomSchema, NomTable):
    """
    Cette fonction va executer la requete SQL, peupler Elasticsearch avec les données retournées et tenir à jour un journal des execution dans une base MySQL sous ce format :
    ID_CONNEXION / NOM_BASE / NOM_SCHEMA / DTM_DERNIERE_EXEC

    :param MySQLCnx: Objet de connexion à la base d'administration MySQL
    :param es: objet de connexion à Elasticsearch
    :param DB: paramètres de connexion à Oracle
    :param Query: Requete à executer dans Oracle
    :param DtmInit: Date d'initialisation si première requete
    :param DocType: type de document dans elasticsearch
    :param Index: nom de l'index dans Elasticsearch
    :param NomBase: Nom de la base requeté
    :param NomSchema: Nom du shema requeté
    :param NomTable: Nom de la table requeté
    :return: None
    """

    # Récupération de la date de dernière execution
    Dtm_chargement = Recup_DTM_Derniere_Exec(MySQLCnx, NomTable)

    if Dtm_chargement == None:

        print '*** CHARGEMENT INITIAL de la table : {0} ***'.format(NomTable)

        # définition d'une date de chargement initial
        Dtm_chargement = DtmInit

        # création du curseur
        cursor = OracleCnx.cursor()

        # préparation de la requete SQL envoyée à Oracle
        SQL = Query.format(Dtm_chargement)

        print SQL

        # Execution de la requete SQL + indexation dans Elasticsearch
        Load_Oracle_to_Elasticsearch(cursor, SQL,  es, DocType, NomTable, Index,MySQLCnx, NomBase, NomSchema, NomTable)



    else:

        print '*** CHARGEMENT INCREMENTAL de la table : {0} ***'.format(NomTable)

        # récupération de la date de dernière execution
        Dtm_chargement = Recup_DTM_Derniere_Exec(MySQLCnx, NomTable)

        print Dtm_chargement

        # création du curseur
        cursor = OracleCnx.cursor()

        # préparation de la requete SQL envoyée à Oracle
        SQL = Query.format(Dtm_chargement)

        print SQL

        # Execution de la requete SQL + indexation dans Elasticsearch
        Load_Oracle_to_Elasticsearch(cursor, SQL,  es, DocType, NomTable, Index, MySQLCnx, NomBase, NomSchema, NomTable)


