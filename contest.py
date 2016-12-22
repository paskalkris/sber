# coding: utf-8
 

import pandas as pd
 

data = pd.read_csv("contest/transactions_test.csv")  # считываем данные
tr_mcc_codes = pd.read_csv("contest/tr_mcc_codes.csv", sep=";")
tr_types = pd.read_csv("contest/tr_types.csv", sep=";")

data['fraud'] = 0
# data['cent_fraud'] = 0
data['cut_fraud'] = 0
data['deposit_fraud'] = 0
data['sec60'] = 0
data['more10da'] = 0
data['trans_mismatch'] = 0
data['doubles'] = 0
data['midnight_minus_fraud'] = 0
data['midnight_plus_fraud'] = 0
data['returns_fraud'] = 0

# Снятие / взнос наличностей в банкоматах с копейками
# cent = data[data['tr_type'].isin([7010, 7011, 7014, 7015, 2010, 2011])]
# cent_fraud = cent[cent['amount'] * 10 % 10 != 0]
# data.loc[cent_fraud.index.values, ['fraud', 'cent_fraud']] = 1

cut = data[(data['tr_type'].isin([2010, 2011]))
           & (data['amount'] * 10 % 10 == 0)]
deposit = data[(data['tr_type'].isin([7010, 7011, 7014, 7015]))
               & (data['amount'] * 10 % 10 == 0)]
cut_fraud = cut[cut['amount'] < -300000]
deposit_fraud = deposit[deposit['amount'] > 1000000]

data.loc[cut_fraud.index.values, ['fraud', 'cut_fraud']] = 1

data.loc[deposit_fraud.index.values, ['fraud', 'deposit_fraud']] = 1




# Число операций, которые выполнены в 60 секунд
sec60 = data[data['tr_datetime'].map(lambda x: x.split(':')[2] == '60')]
data.loc[sec60.index.values, ['fraud', 'sec60']] = 1



# больше 10 операций в одно и тоже время на одну и туже сумму
cust_count = data.groupby(['tr_datetime', 'amount'], as_index=False)['customer_id'].count()
dt2 = pd.DataFrame(cust_count[cust_count['customer_id'] > 3])

data.loc[(pd.concat([data, dt2], 
                keys=['tr_datetime', 'amount'], 
                axis=1)['amount']['tr_datetime'].isnull() == False), ['fraud', 'more10da']] = 1



# Несоответствие кодов друг другу по смыслу
data.loc[(data['mcc_code'] == 6011) &
                      (data['tr_type'].isin([7011, 7015, 7014, 7010])) |
                      (data['mcc_code'] == 6010) &
                      (data['tr_type'].isin([7021, 7020, 7025, 7024, 2020, 2021, 7040, 7041,
                                             7030, 7031, 7070, 7071, 7044, 7034, 7035, 7074,
                                             7075, 4041, 4020, 4021, 1010, 1000])), 
         ['fraud', 'trans_mismatch']] = 1



# Полные дубли
dt3 = pd.DataFrame(data.groupby(['customer_id', 'tr_datetime',
                        'mcc_code', 'tr_type', 'amount'], as_index=False)['fraud'].count())
doubles = dt3[dt3['fraud'] > 1]
data.loc[(pd.concat([data, doubles], 
                keys=['customer_id', 'tr_datetime', 'mcc_code', 'tr_type', 'amount'], 
                axis=1)['tr_datetime']['tr_datetime'].isnull() == False), ['fraud', 'doubles']] = 1
 


# Полуночники
midnight = data[data['tr_datetime'].map(lambda x: x.split(' ')[1] == '00:00:00')]
midnight_cnt_minus = pd.DataFrame(midnight[midnight['amount'] < 0]
                                  .groupby(['customer_id', 'tr_datetime'], as_index=False)['mcc_code'].count())
midnight_cnt_plus = pd.DataFrame(midnight[midnight['amount'] > 0]
                                 .groupby(['customer_id', 'tr_datetime'], as_index=False)['mcc_code'].count())

midnight_minus_fraud = midnight_cnt_minus[midnight_cnt_minus['mcc_code'] > 1]
midnight_plus_fraud = midnight_cnt_plus[midnight_cnt_plus['mcc_code'] > 1]

data.loc[(pd.concat([data, midnight_minus_fraud], 
                keys=['customer_id', 'tr_datetime'], 
                axis=1)['tr_datetime']['tr_datetime'].isnull() == False) & (data['amount'] < 0),
         ['fraud', 'midnight_minus_fraud']] = 1
data.loc[(pd.concat([data, midnight_plus_fraud], 
                keys=['customer_id', 'tr_datetime'], 
                axis=1)['tr_datetime']['tr_datetime'].isnull() == False) & (data['amount'] > 0), 
         ['fraud', 'midnight_plus_fraud']] = 1


# Возвраты
returns = (data[data['tr_type'].isin([6000, 6010, 6100, 6110, 6200, 6210])]
           .groupby('customer_id', as_index=False)['tr_datetime'].count())
data.loc[(data['tr_type'].isin([6000, 6010, 6100, 6110, 6200, 6210]) & 
      (data['customer_id'].isin(returns[returns['tr_datetime'] >= 10]['customer_id']))), 
         ['fraud', 'returns_fraud']] = 1

data.to_csv('result/result_items.csv')
data['fraud'].to_csv('result/result.csv')

print('Done!!!!!!!!!!!!!!!!!!!!')
print('Frauds: ', len(data[data['fraud'] == 1]))
