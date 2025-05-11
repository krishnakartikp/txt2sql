#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 30 22:30:06 2025

@author: kartik
"""

import streamlit as st
import pandas as pd
import psycopg2

from langchain.embeddings.openai import OpenAIEmbeddings as OAIE
from langchain.vectorstores import FAISS as FS
import os

from langchain.embeddings.openai import OpenAIEmbeddings
from langchain.vectorstores import FAISS 

from langchain.chains.question_answering import load_qa_chain
from langchain.llms import OpenAI

st.title("Natural Querying")

con_string = st.text_input("Enter your Database Connection string:")


nat_query = st.text_input("Enter your Natural Query:")

if st.button("Get query & data"):
    if con_string and nat_query:
        
        query_tables = "select table_name from information_schema.tables where table_schema = 'public'"
        
        query_columns = "select table_name,column_name,ordinal_position,is_nullable,data_type,character_maximum_length as maxlen from information_schema.columns where table_schema = 'public'"
        
        query_relations = """
            SELECT 
                tc.table_name as pri_tbl_name, 
                kcu.column_name as pri_col_name, 
                ccu.table_name as sec_tbl_name, 
                ccu.column_name as sec_col_name
            FROM information_schema.table_constraints AS tc 
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY' 
                AND tc.table_schema='public'        
        """
        conn = psycopg2.connect(con_string)
        cur = conn.cursor()

        cur.execute(query_tables)
        sch_tbl = pd.DataFrame(cur.fetchall(), columns = ['table_name'])
        
        cur.execute(query_columns)
        sch_col = pd.DataFrame(cur.fetchall(), columns = ['table_name', 'column_name','ordinal_position', 'is_nullable', 'data_type', 'maxlen'])
        
        cur.execute(query_relations)
        sch_rel = pd.DataFrame(cur.fetchall(), columns = ['pri_tbl_name', 'pri_col_name','sec_tbl_name', 'sec_col_name'])
        
        conn.close()

        dict1 = {'NO':' NOT NULL','YES':' NULL'}
        sch_col["txt"] = ''
        sch_col.loc[sch_col.maxlen.notna(),"txt"] = '(' + sch_col[sch_col.maxlen.notna()].maxlen.astype(int).astype(str) + ')'
        sch_col["txt"] = sch_col.column_name + ' ' + sch_col.data_type + sch_col.txt + sch_col.is_nullable.map(dict1)
        
        sch_rel["txt"] = sch_rel.pri_tbl_name + ' -> ' + sch_rel.pri_col_name + ' == ' + sch_rel.sec_tbl_name + ' -> ' + sch_rel.sec_col_name


        db_metadata = pd.DataFrame(columns=["table_name", "schema"])
        
        for i in range(len(sch_tbl)):
            var_tbl = str(sch_tbl.iloc[i,0])
            var_sch = ('Table Schema : \n CREATE TABLE '+ var_tbl + '(' 
                       + sch_col.loc[sch_col.table_name == var_tbl,"txt"].str.cat(sep=", ") + ')' 
                       + '\n\nTable Relationships : \n Format: [Primary key table] -> [Primary key column] == [Foreign key table] -> [Foreign key column] \n ' 
                       + sch_rel.loc[(sch_rel.pri_tbl_name == var_tbl) | (sch_rel.sec_tbl_name == var_tbl),"txt"].str.cat(sep=", \n ")
                      )
            db_metadata.loc[i] = [var_tbl, var_sch]

        mydocs = list(db_metadata["schema"])
        
        os.environ["OPENAI_API_KEY"] = "**************"

        embeddings = OpenAIEmbeddings()
        
        doc_search = FAISS.from_texts(mydocs, embeddings)

        #query = "genre wise total billing amount in decreasing order"
        
        docs = doc_search.similarity_search(nat_query)

        chain = load_qa_chain(OpenAI(), chain_type="stuff")
        nat_sql_query = "generate postgres sql query for -> " + nat_query
        
        result_query = chain.run(input_documents=docs, question=nat_sql_query)
        
        st.code(result_query, language="sql")
        
            
        conn = psycopg2.connect(con_string)
        cur = conn.cursor()
        cur.execute(result_query)
        cols = [desc[0] for desc in cur.description]
        result_data = pd.DataFrame(cur.fetchall(),columns=cols)
        conn.close()
        st.dataframe(result_data)

    else:
        st.warning("Please provide both the input!")     
        
