import pandas as pd
import spacy
import shutil
import os

df1 = pd.read_csv('/var/www/html/scrapper/PCGA/utils/pcga-act-tokens.csv')
df2 = pd.read_csv('/var/www/html/scrapper/PCGA/utils/pcga-pas-tokens.csv')
df3 = pd.read_csv('/var/www/html/scrapper/PCGA/utils/pcga-pat-tokens.csv')
df4 = pd.read_csv('/var/www/html/scrapper/PCGA/utils/pcga-eerr-tokens.csv')

def writeExcel(documents):
    for account in documents:
        print(account)
        print('\n')

def copy_rename(new_file_name):
        dst_dir= '/var/www/html/scrapper/public/reports/PCGA'
        src_file = '/var/www/html/scrapper/PCGA/pcga-template.xlsx'
        shutil.copy(src_file, dst_dir)
        
        dst_file = '/var/www/html/scrapper/public/reports/PCGA/pcga-template.xlsx'
        new_dst_file_name = os.path.join(dst_dir, new_file_name)
        os.rename(dst_file, new_dst_file_name)

def matchCellPCGA(work_documents, thread):
    if (thread == 1):
        work_documents = work_documents[:int(len(work_documents)/2)]
    elif (thread == 2):
        work_documents = work_documents[int(len(work_documents)/2):]
    nlp = spacy.load('es')
    new_documents = []
    for account in work_documents:
        group = int(float(account['group']))
        text = account['clean_text'].strip()
        doc = nlp(text)
        new_text = ''

        for token in doc:
            if (len(token.lemma_) <= 1):
                pass

            else:
                new_text = new_text+' '+str(token.lemma_)

        account['clean_text'] = new_text.strip()

        if(group == 1):
            total_simil = []
            headers = []
            doc1 = nlp(account['clean_text'])
            for column in df1:
                headers.append(column)
                aux_simil = []
                for x in df1[column]:
                    if (type(x) == str):
                        doc2 = nlp(x)
                        simil = doc1.similarity(doc2)
                        if (simil == 1.0):
                            aux_simil.append(simil)
                            break 
                        else:
                            aux_simil.append(simil)
                    else:
                        break
                total_simil.append(max(aux_simil))
            cell = headers[total_simil.index(max(total_simil))]
            account['target_cell'] = cell

        elif(group == 2):
            total_simil = []
            headers = []
            doc1 = nlp(account['clean_text'])
            for column in df2:
                headers.append(column)
                aux_simil = []
                for x in df2[column]:
                    if (type(x) == str):
                        doc2 = nlp(x)
                        simil = doc1.similarity(doc2)
                        if (simil == 1.0):
                            aux_simil.append(simil)
                            break 
                        else:
                            aux_simil.append(simil)
                    else:
                        break
                total_simil.append(max(aux_simil))
            cell = headers[total_simil.index(max(total_simil))]
            account['target_cell'] = cell

        elif(group == 3):
            total_simil = []
            headers = []
            doc1 = nlp(account['clean_text'])
            for column in df3:
                headers.append(column)
                aux_simil = []
                for x in df3[column]:
                    if (type(x) == str):
                        doc2 = nlp(x)
                        simil = doc1.similarity(doc2)

                        if (simil == 1.0):
                            aux_simil.append(simil)
                            break 
                        else:
                            aux_simil.append(simil)
                    else:
                        break
                total_simil.append(max(aux_simil))
            cell = headers[total_simil.index(max(total_simil))]
            account['target_cell'] = cell

        elif(group == 4):
            total_simil = []
            headers = []
            doc1 = nlp(account['clean_text'])
            for column in df4:
                headers.append(column)
                aux_simil = []
                for x in df4[column]:
                    if (type(x) == str):
                        doc2 = nlp(x)
                        simil = doc1.similarity(doc2)

                        if (simil == 1.0):
                            aux_simil.append(simil)
                            break 
                        else:
                            aux_simil.append(simil)
                    else:
                        break
                total_simil.append(max(aux_simil))
            cell = headers[total_simil.index(max(total_simil))]
            account['target_cell'] = cell

        new_documents.append(account)

    writeExcel(new_documents)

                    
        

    