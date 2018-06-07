import pandas as pd
import pickle

def cleanData(df):
    df = df.apply(lambda y: y.str.replace('[0-9]*-[0-9]*-[0-9]*\s', '') if(y.dtype == 'object') else y)
    df = df.apply(lambda y: y.str.replace('[0-9]', '') if(y.dtype == 'object') else y)
    df = df.apply(lambda y: y.str.lower() if(y.dtype == 'object') else y)
    df = df.apply(lambda y: y.str.replace('.', ' ') if(y.dtype == 'object') else y)
    df = df.apply(lambda y: y.str.replace('-', ' ') if(y.dtype == 'object') else y)
    df = df.apply(lambda y: y.str.replace('vta ', 'venta ') if(y.dtype == 'object') else y)
    df = df.apply(lambda y: y.str.replace(' x ', ' por ') if(y.dtype == 'object') else y)
    df = df.apply(lambda y: y.str.replace('cta', 'cuenta ') if(y.dtype == 'object') else y)
    df = df.apply(lambda y: y.str.replace('tarj ', 'tarjeta ') if(y.dtype == 'object') else y)
    df = df.apply(lambda y: y.str.replace('impto ', 'impuesto ') if(y.dtype == 'object') else y)
    df = df.apply(lambda y: y.str.replace('impto', 'impuesto') if(y.dtype == 'object') else y)
    df = df.apply(lambda y: y.str.replace('doctos ', 'documentos ') if(y.dtype == 'object') else y)
    df = df.apply(lambda y: y.str.replace('asign ', 'asignación ') if(y.dtype == 'object') else y)
    df = df.apply(lambda y: y.str.replace('remuner$', 'remuneración') if(y.dtype == 'object') else y)
    df = df.apply(lambda y: y.str.replace('leas ', 'leasing ') if(y.dtype == 'object') else y)
    df = df.apply(lambda y: y.str.replace('dif ', 'diferido ') if(y.dtype == 'object') else y)
    df = df.apply(lambda y: y.str.replace('rete$', 'retención') if(y.dtype == 'object') else y)
    df = df.apply(lambda y: y.str.replace('prov ', 'proveedor ') if(y.dtype == 'object') else y)
    df = df.apply(lambda y: y.str.replace('2da', 'segunda') if(y.dtype == 'object') else y)
    df = df.apply(lambda y: y.str.replace('obl ', 'obligaciones ') if(y.dtype == 'object') else y)
    df = df.apply(lambda y: y.str.replace('bcos ', 'bancos ') if(y.dtype == 'object') else y)
    df = df.apply(lambda y: y.str.replace('ins ', 'instituciones ') if(y.dtype == 'object') else y)
    df = df.apply(lambda y: y.str.replace('finan ', 'financieras ') if(y.dtype == 'object') else y)
    df = df.apply(lambda y: y.str.replace('fact ', 'facturas ') if(y.dtype == 'object') else y)
    df = df.apply(lambda y: y.str.replace('fxr ', 'fondo por rendir ') if(y.dtype == 'object') else y)
    df = df.apply(lambda y: y.str.replace('naci$', 'nacional') if(y.dtype == 'object') else y)
    df = df.apply(lambda y: y.str.replace('bco ', 'banco ') if(y.dtype == 'object') else y)
    df = df.apply(lambda y: y.str.replace('credito', 'crédito') if(y.dtype == 'object') else y)
    df = df.apply(lambda y: y.str.replace('prestamo', 'préstamo') if(y.dtype == 'object') else y)
    df = df.apply(lambda y: y.str.replace('invers ', 'inversión ') if(y.dtype == 'object') else y)
    df = df.apply(lambda y: y.str.replace('fdo ', 'fondo ') if(y.dtype == 'object') else y)
    df = df.apply(lambda y: y.str.replace('vehiculo', 'vehículo') if(y.dtype == 'object') else y)
    df = df.apply(lambda y: y.str.replace('serv ', 'servicio ') if(y.dtype == 'object') else y)
    df = df.apply(lambda y: y.str.replace('mu$', 'mutuos') if(y.dtype == 'object') else y)
    df = df.apply(lambda y: y.str.replace('op$', 'operacionales') if(y.dtype == 'object') else y)
    df = df.apply(lambda y: y.str.replace('difer ', 'diferencia ') if(y.dtype == 'object') else y)
    df = df.apply(lambda y: y.str.replace('gtos ', 'gastos ') if(y.dtype == 'object') else y)
    df = df.apply(lambda y: y.str.replace('indem ', 'indemnización ') if(y.dtype == 'object') else y)
    df = df.apply(lambda y: y.str.replace('ingr ', 'ingreso ') if(y.dtype == 'object') else y)
    df = df.apply(lambda y: y.str.replace('ope$', 'operacional') if(y.dtype == 'object') else y)
    df = df.apply(lambda y: y.str.replace('c monetaria ', 'correción monetaria ') if(y.dtype == 'object') else y)
    df = df.apply(lambda y: y.str.replace('1a ', 'primera ') if(y.dtype == 'object') else y)
    df = df.apply(lambda y: y.str.replace('amortiz ', 'amortización ') if(y.dtype == 'object') else y)
    df = df.apply(lambda y: y.str.replace('cuente ', 'cuenta ') if(y.dtype == 'object') else y)
    df = df.apply(lambda y: y.str.replace('cuent ', 'cuenta ') if(y.dtype == 'object') else y)
    df = df.apply(lambda y: y.str.replace('maquin$', 'maquinaria') if(y.dtype == 'object') else y)
    df = df.apply(lambda y: y.str.replace('cte ', 'corriente ') if(y.dtype == 'object') else y)
    df = df.apply(lambda y: y.str.replace('/', ' ') if(y.dtype == 'object') else y)
    df = df.apply(lambda y: y.str.replace('(', '') if(y.dtype == 'object') else y)
    df = df.apply(lambda y: y.str.replace(')', '') if(y.dtype == 'object') else y)
    df = df.apply(lambda y: y.str.replace('$', '') if(y.dtype == 'object') else y)
    df = df.apply(lambda y: y.str.replace('  ', ' ') if(y.dtype == 'object') else y)
    df = df.apply(lambda y: y.str.strip() if(y.dtype == 'object') else y)
    return df

def saveData(data, file_name):
    with open('models/'+file_name+'.pickle', 'wb') as f:
        pickle.dump(data, f)

def loadData(filename):
    with open(filename, 'rb') as f:
        model = pickle.load(f)
    return model

def ignore(data):
    if (float(data['debts']) == 0 and float(data['credits']) == 0 and 
        float(data['debtor']) == 0 and float(data['creditor']) == 0 and 
        float(data['assets']) == 0 and float(data['liabilities']) == 0 and 
        float(data['lost']) == 0 and float(data['gain']) == 0):
        return True
    else:
        return False

def cleanNonsense(data):
    len_a = len(data)
    sigue = True
    while(sigue):
        for x in data:
            if (ignore(x) == True):
                data.remove(x)
            else:
                pass
        len_b = len(data)
        if(len_b < len_a):
            len_a = len_b
        else:
            sigue = False
    return data