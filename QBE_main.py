import logging
import logging.config
from os.path import join
from numpy import NaN, inner
import numpy as np
from numpy.lib.function_base import insert
import pandas as pd
#from pandas.core.frame import DataFrame
import common.function.convertTXTtoANSI as convertANSI
from config import settings
import glob
#from simpledbf import Dbf5
from datetime import *
from datetime import timedelta
from datetime import datetime
from string import Template
import os
from common.function.util import calculate_age
import csv
import msoffcrypto
import io
from qbe_output import qbeOutput, qbeOutputToTemplate
import sys
class qbe:
  
  def __init__(self, sub_path, qbe_output):
    self.data_input_path = self.set_data_input_path( sub_path)  
    self.data_output_path = self.set_data_output_path( sub_path)   
    convertANSI.convertTXTToANSI(self.data_input_path)
    self.actmb = self.set_actmb( sub_path)
    self.mms_cbbe = self.set_mms_cbbe()
    self.mixed = self.set_mixed()
    self.termb = self.set_termb()
    self.qbe_output = qbe_output
    self.err_df = pd.DataFrame( columns = ["POLICY NO","DESC"])

  def append_err_df(self, staff_ids, desc):
    df = pd.DataFrame(data=staff_ids)
    df['DESC'] = desc
    self.err_df = self.err_df.append(df,ignore_index=True )

  def set_data_input_path(self, sub_path):
    #print (settings['qbe']['path']['input_data'])
    return settings['qbe']['path']['input_data'] + sub_path[:4] + '/' + sub_path[4:6] + '/' + sub_path[6:] + '/'
  def set_data_output_path(self, sub_path):
    return settings['qbe']['path']['output_data'] + sub_path[:4] + '/' + sub_path[4:6] + '/' + sub_path[6:] + '/'

  def set_actmb(self,sub_path):
      #files = glob.glob(self.data_input_path+"/UMP*.xls")
      actmb_header = settings['qbe']['data']['actmb_header']
      actmb = pd.DataFrame(columns = actmb_header)
      files = (self.data_input_path+'UMP'+sub_path[6:]+sub_path[4:6]+sub_path[:4]+'.xls')
      try:
        actmb = pd.read_excel(files,names=actmb_header)
        #print (actmb)
      except:
        qbe_logger.exception("message")     
        raise Exception(tag + "actmb error")
      return actmb

  def set_mms_cbbe(self):
    mb_cbbe_header = settings['data']['mb_cbbe_header']
    date_cols = settings['data']['mb_cbbe_date_cols'] 
    try:
      mms_cbbe = pd.read_csv(self.data_input_path+'mms.txt', sep='\t',  names = mb_cbbe_header, parse_dates=date_cols, dayfirst=True,dtype=str)
      qbe_output.addfile('mms.txt', len(mms_cbbe.index))
      mms_cbbe = mms_cbbe.fillna("").applymap(lambda x: x.strip() if isinstance(x, str) else x)
      #print (mms_cbbe)
    except:
      qbe_logger.exception("message")     
      raise Exception(tag + "mms_cbbe error")      
    return mms_cbbe

  def set_mixed(self):

    actmb = self.actmb
    mixed_header = settings['qbe']['data']['mixed_header']
    mixed = pd.DataFrame(columns = mixed_header)
    diff_header = settings['qbe']['data']['diff_header']
    diff = pd.DataFrame(columns = diff_header)
    new_header = settings['qbe']['data']['new_header']
    new = pd.DataFrame(columns = new_header)

    try:
      #append from actmb
      mixed = mixed.append(actmb)
      #replace all mb_ter with 'N',nw_co_cde with 'QBEI00011718',mb_medipas with 'N', 
      mixed['MB_TER'] = 'N'
      mixed['NW_CO_CDE'] = 'QBEI00011718'
      mixed['MB_MEDIPAS'] = 'N'
      #mb_class with 'E', mb_policy with policy_no,mb_name with upper(name),
      mixed['MB_CLASS'] = 'E'
      mixed['MB_POLICY'] = mixed['POLICY_NO']
      mixed['MB_NAME'] = mixed['NAME'].str.upper()
      #mb_dept with expiry_Dt
      mixed['MB_DEPT'] = mixed['EXPIRY_DT']
      #mb_jo_dt with ctod(left(com_Dt,2)+'/'+substr(com_Dt,4,2)+'/'+right(com_Dt,4)),mb_term_dt with ctod(left(expiry_Dt,2)
      # +'/'+substr(expiry_Dt,4,2)+'/'+right(expiry_Dt,4)),mb_bir_dt with ctod(left(dob,2)+'/'+substr(dob,4,2)+'/'+right(dob,4)),
      mixed['COM_DT'] = mixed['COM_DT'].map(lambda x: x.strftime('%d/%m/%Y'))
      mixed['EXPIRY_DT'] = mixed['EXPIRY_DT'].map(lambda x: x.strftime('%d/%m/%Y'))
      mixed['DOB'] = mixed['DOB'].map(lambda x: x.strftime('%d/%m/%Y'))
      mixed['MB_JO_DT'] = mixed['COM_DT']
      mixed['MB_TERM_DT'] = mixed['EXPIRY_DT']
      mixed['MB_BIR_DT'] = mixed['DOB']
      #mb_staffid with id,mb_cert_no with mbship_no, mb_plan with plan
      mixed['MB_STAFFID'] = mixed['ID']
      mixed['MB_CERT_NO'] = mixed['MBSHIP_NO']
      mixed['MB_PLAN'] = mixed['PLAN']
      #replace all nw_sch_cde with 'QBE1-003' for plan ='1',replace all nw_sch_cde with 'QBE2-003' for plan ='2'
      for i in range (len(mixed)):
        if mixed['PLAN'][i] == 1:
          mixed.loc[i, 'NW_SCH_CDE'] = "QBE1-003"
        elif mixed['PLAN'][i] == 2:
          mixed.loc[i, 'NW_SCH_CDE'] = "QBE2-003"
        else:continue
      #select * from a where inlist(trans_type,'NP','CN','CV','CB')
      #copy to new.dbf type fox2x
      #indicate row k in df new
      k = 0
      for j in range (len(mixed)):
        if (mixed['TRANS_TYPE'][j] == 'NP')|(mixed['TRANS_TYPE'][j] == 'CN')|(mixed['TRANS_TYPE'][j] == 'CV')|(mixed['TRANS_TYPE'][j] == 'CB'):
          new.loc[k] = mixed.iloc[j]
          k += 1
        else:continue
      self.new = new
      #select * from a where trans_type<>'NP'
      #copy to diff.dbf type fox2x
      #indicate row k in df diff
      k = 0
      for j in range (len(mixed)):
        if mixed['TRANS_TYPE'][j] != 'NP':
          diff.loc[k] = mixed.iloc[j]
          k += 1
        else:continue
      self.diff = diff
      #print (mixed)
    except:
      qbe_logger.exception("message")     
      raise Exception(tag + "mixed error")
    return mixed


  def set_termb(self):
    termb_header = settings['qbe']['data']['termb_header']
    termb = pd.DataFrame(columns = termb_header)
    twotwod_header = settings['qbe']['data']['diff_header']
    twotwod = pd.DataFrame(columns = twotwod_header)
    twotwof_header = settings['qbe']['data']['diff_header']
    twotwof = pd.DataFrame(columns = twotwof_header)
    try:
      #use diff alias a
      diff = self.diff
      #select * from a where inlist(trans_type,'CN','CP')
      k = 0
      for j in range (len(diff)):
        if (diff['TRANS_TYPE'][j] == 'CN')|(diff['TRANS_TYPE'][j] == 'CP'):
          #copy to 22d.dbf type fox2x
          twotwod.loc[k] = diff.iloc[j]
          k += 1
        else:continue
      self.twotwod = twotwod

      #select * from a where inlist(trans_type,'CD','CS','CV','OO','CB')
      k = 0
      for i in range (len(diff)):
        if (diff['TRANS_TYPE'][i] == 'CD')|(diff['TRANS_TYPE'][i] == 'CS')|(diff['TRANS_TYPE'][i] == 'CV')|(diff['TRANS_TYPE'][i] == 'OO')|(diff['TRANS_TYPE'][i] == 'CB'):
          #copy to 22f.dbf type fox2x
          twotwof.loc[k] = diff.iloc[i]
          k += 1
        else:continue
      self.twotwof = twotwof

      #use 22d alias a
      df_a = self.twotwod
      #use mb_cbbe alias b
      df_b = self.mms_cbbe

      #select a.*,b.mb_name,b.mb_jo_dt,b.mb_term_dt,b.nw_sch_cde,b.nw_mb_cde,b.mb_ter,b.mb_crea_dt,b.mb_lstupd from a left outer join b on alltrim(a.id)==alltrim(b.mb_staffid)
      twotwod_mms_cbbe_result_1 = df_a.merge(df_b, how='outer', left_on=['ID'], right_on=['MB_STAFFID'])
      result_1 = twotwod_mms_cbbe_result_1.loc[:,['NW_CO_CDE_x','NW_SCH_CDE_x','MB_NAME_x','MB_CLASS_x','MB_JO_DT_x','MB_TERM_DT_x','MB_TER_x','MB_SEX_x','MB_ID_x','MB_CERT_NO_x',
                                              'MB_STAFFID_x','MB_BIR_DT_x','MB_BANK_AC_x','MB_PLAN_x','MB_POLICY_x','MB_INPATNT_x','MB_REINSUR_x','MB_DEPT_x','MB_ADDR1_x',
                                              'MB_ADDR2_x','MB_ADDR3_x','MB_ADDR4_x','MB_TEL_x','MB_PRECON1_x','MB_PRECON2_x','MB_PRECON3OCODE','MB_MEDIPAS_x','MB_OK',
                                              'POLICY_NO','NAME','MBSHIP_NO','ID','COM_DT','EXPIRY_DT','TRANS_TYPE','FLAG','OLD_TER_DT','DOB','PLAN','MB_NAME_y','MB_JO_DT_y',
                                              'MB_TERM_DT_y','NW_SCH_CDE_y','NW_MB_CDE','MB_TER_y','MB_CREA_DT','MB_LSTUPD']]
      if len(result_1.index) > 0:    
        qbe_output.add_msg('err: TERMB: select a.*,b.mb_name,b.mb_jo_dt,b.mb_term_dt,b.nw_sch_cde,b.nw_mb_cde,b.mb_ter,b.mb_crea_dt,b.mb_lstupd from a left outer join b on alltrim(a.id)==alltrim(b.mb_staffid)') 
        #self.append_err_df(result['NW_CO_CDE_x'].drop_duplicates().rename('NW_CO_CDE_x'),"select a.*,b.mb_name,b.mb_jo_dt,b.mb_term_dt,b.nw_sch_cde,b.nw_mb_cde,b.mb_ter,b.mb_crea_dt,b.mb_lstupd from a left outer join b on alltrim(a.id)==alltrim(b.mb_staffid)")

      #select a.mb_name,b.nw_mb_cde,' ' as mb_ok,'Y' as mb_ter,a.com_Dt, a.expiry_dt,b.mb_jo_dt,b.mb_term_dt ,b.mb_ter  from a inner join b on alltrim(a.policy_no)+alltrim(a.mbship_no)==alltrim(b.mb_policy)+alltrim(b.mb_cert_no)
      twotwod_mms_cbbe_result_2 = df_a.merge(df_b, how='inner', left_on=['POLICY_NO','MBSHIP_NO'], right_on=['MB_POLICY','MB_CERT_NO'])
      twotwod_mms_cbbe_result_2['MB_OK'] = ' '
      twotwod_mms_cbbe_result_2['MB_TER_x'] = 'Y'
      result_2 = twotwod_mms_cbbe_result_2.loc[:,['MB_NAME_x','NW_MB_CDE','MB_OK','MB_TER_x','COM_DT','EXPIRY_DT','MB_JO_DT_y','MB_TERM_DT_y','MB_TER_y']]
      if len(result_2.index) > 0:
        qbe_output.add_msg('err: TERMB: select a.mb_name,b.nw_mb_cde,' ' as mb_ok,"Y" as mb_ter,a.com_Dt, a.expiry_dt,b.mb_jo_dt,b.mb_term_dt ,b.mb_ter  from a inner join b on alltrim(a.policy_no)+alltrim(a.mbship_no)==alltrim(b.mb_policy)+alltrim(b.mb_cert_no)')

      #select b.nw_mb_cde,b.mb_jo_dt as mb_jo_dt, b.mb_term_dt,'Y' as mb_ter,' ' as mb_ok,a.old_ter_dt  as nw_term_dt,expiry_dt,a.mb_policy,b.mb_name as old_name,a.mb_name as new_name  from a inner join b on alltrim(a.policy_no)+alltrim(a.mbship_no)==alltrim(b.mb_policy)+alltrim(b.mb_cert_no) and (b.mb_ter ='N' or b.mb_ter=' ') order by a.mb_policy,a.mb_jo_dt
      twotwod_mms_cbbe_result = df_a.merge(df_b, how='inner', left_on=['POLICY_NO','MBSHIP_NO'], right_on=['MB_POLICY','MB_CERT_NO']).sort_values(['MB_POLICY_x','MB_JO_DT_x'])
      twotwod_mms_cbbe_result['MB_JO_DT_x'] = twotwod_mms_cbbe_result['MB_JO_DT_y']
      twotwod_mms_cbbe_result['MB_TER_x'] = 'Y'
      twotwod_mms_cbbe_result['MB_OK'] = ' '
      twotwod_mms_cbbe_result = twotwod_mms_cbbe_result.rename(columns={'OLD_TER_DT': 'NW_TERM_DT'})
      twotwod_mms_cbbe_result = twotwod_mms_cbbe_result.rename(columns={'MB_NAME_y': 'OLD_NAME'})      
      twotwod_mms_cbbe_result = twotwod_mms_cbbe_result.rename(columns={'MB_NAME_x': 'NEW_NAME'})
      result = twotwod_mms_cbbe_result.loc[:,['NW_MB_CDE','MB_JO_DT_x','MB_TERM_DT_y','MB_TER_x','MB_OK','NW_TERM_DT','EXPIRY_DT','MB_POLICY_x','OLD_NAME','NEW_NAME']]  
      #copy to term.dbf type fox2x
      termb = result
      #use term alias a
      df_a = termb
      #delete for (nw_term_dt<>'?' and mb_term_dt<ctod(nw_term_dt)) or (nw_term_dt='?' and mb_term_dt<ctod(expiry_dt))
      for j in range (len(df_a)):
        if ((df_a['NW_TERM_DT'][j] != '?' & df_a['MB_TERM_DT_x'][j]<df_a['NW_TERM_DT'][j])|(df_a['NW_TERM_DT'][j] == '?' & df_a['MB_TERM_DT_x'][j]<df_a['EXPIRY_DT'][j])):
          df_a.drop([j])
        else:continue

      for i in range (len(df_a)):
        #replace mb_term_dt with ctod(nw_term_dt) for nw_term_dt<>'?' and between(ctod(nw_term_dt),mb_jo_dt,mb_term_dt)
        if (df_a['NW_TERM_DT'][i] != '?' & df_a['NW_TERM_DT'][i].between(df_a['MB_JO_DT_x'][i],df_a['MB_TERM_DT_y'][i])):
          df_a['MB_TERM_DT_y'][i] = df_a['NW_TERM_DT'][i]
      for i in range (len(df_a)):
        #replace mb_term_dt with ctod(expiry_dt) for nw_term_dt='?' and between(ctod(expiry_dt),mb_jo_dt,mb_term_dt)
        if (df_a['NW_TERM_DT'][i] == '?' & df_a['EXPIRY_DT'][i].between(df_a['MB_JO_DT_x'][i],df_a['MB_TERM_DT_y'][i])):
          df_a['MB_TERM_DT_y'][i] = df_a['EXPIRY_DT'][i]
      for i in range (len(df_a)):
        #replace mb_term_dt with mb_jo_dt for (nw_term_dt<>'?' and mb_jo_dt>ctod(nw_term_dt)) or (nw_term_dt='?' and mb_jo_dt>ctod(expiry_dt))   
        if ((df_a['NW_TERM_DT'][i] != '?' & df_a['MB_JO_DT_x'][i] > df_a['NW_TERM_DT'][i]) | (df_a['NW_TERM_DT'][i] == '?' & df_a['MB_JO_DT_x'][i] > df_a['EXPIRY_DT'][i])):
          df_a['MB_TERM_DT_y'][i] = df_a['MB_JO_DT_x'][i]

      #select * from a where mb_term_dt < mb_jo_dt
      df_a = df_a[df_a['MB_TERM_DT_y']<df_a['MB_JO_DT_x']]
      for i in range (len(df_a)):
        #update a set mb_term_dt = mb_jo_dt where mb_term_dt < mb_jo_dt
        if df_a['MB_TERM_DT_y'][i] < df_a['MB_JO_DT_x'][i]:
          df_a['MB_TERM_DT_y'][i] = df_a['MB_JO_DT_x'][i]
          
    except:
      qbe_logger.exception("message")
      raise Exception(tag + "termb error")
    return termb


  def validate(self):
    #check duplicate
    check_mms_cbbe_duplicate = self.check_mms_cbbe_duplicate()
    check_mms_cbbe = self.check_mms_cbbe()
    check_actmb = self.check_actmb()

   # if not (check_mms_cbbe_duplicate             
   #         & check_mms_cbbe
   #         & check_actmb):
   #   raise Exception(tag + "stop for validation")  
    
  def check_mms_cbbe_duplicate(self):
    #check duplicate
    #select * from a where mb_Ter='N' group by mb_policy, mb_term_dt having count(*) >1 order by mb_crea_dt
    df = self.mms_cbbe
    dup = df[df['MB_TER'] == 'N'].groupby(['MB_POLICY', 'MB_TERM_DT']).count().reset_index()

    duplicate = (dup[dup['MB_NAME']>1]).sort_values(['MB_CREA_DT'])
    #print(duplicate)
    if len(duplicate.index) > 0:
      list = df[df.set_index(['MB_POLICY','MB_TERM_DT']).index.isin(duplicate.set_index(['MB_POLICY','MB_TERM_DT']).index)]
      qbe_output.add_msg('err: mms_cbbe: Duplicate found')
      self.append_err_df(list['MB_POLICY'].drop_duplicates().rename('MB_POLICY'),"mms_cbbe: Duplicate found")
    
    #return false if count > 0  
    check_duplicate_result = len(duplicate.index) == 0
    return check_duplicate_result
  
  def check_mms_cbbe(self):
    df = self.mms_cbbe
    # check empty mb_term_date exist
    #select * from d where empty(mb_term_dt)
    empty_mb_term_date = df[(pd.isnull(df['MB_TERM_DT']))] 
    
    if len(empty_mb_term_date.index) > 0:    
      qbe_output.add_msg('err: mms_cbbe: empty mb_term_date found') 
      self.append_err_df(empty_mb_term_date['MB_STAFFID'].drop_duplicates().rename('STAFFID'),"mms_cbbe: empty mb_term_date found") 

    # check join term date
    #select * from d where mb_term_dt>date() and mb_term_dt+1<>ctod(left(dtoc(mb_jo_dt),6)+alltrim(str(year(mb_jo_dt)+1))) and mb_jo_dt<>mb_term_dt and mb_ter='N'
    date_invalid = df[(df['MB_TERM_DT']> today)
                      & (df['MB_TERM_DT']!=(df['MB_JO_DT'] + pd.offsets.DateOffset(years=1)- pd.offsets.DateOffset(days=1)))
                      & (df['MB_TERM_DT']!=df['MB_JO_DT'])
                      & (df['MB_TER']=='N')] 

    if len(date_invalid.index) > 0:    
      qbe_output.add_msg('err: mms_cbbe: term date invalid found') 
      self.append_err_df(date_invalid['MB_STAFFID'].drop_duplicates().rename('STAFFID'),"mms_cbbe: term date invalid found") 
      
    # check empty data exist
    #select * from d where mb_term_dt>date() and (empty(mb_name) or empty(mb_staffid) or empty(mb_bir_dt) or empty(mb_plan) or empty(mb_dept))
    empty_data = df[(df['MB_TERM_DT']> today)
                    &(
                      (pd.isnull(df['MB_NAME']))
                      |(pd.isnull(df['MB_STAFFID']))
                      |(pd.isnull(df['MB_BIR_DT']))
                      |(pd.isnull(df['MB_PLAN']))
                      |(pd.isnull(df['MB_DEPT']))
                    )]
    if len(empty_data.index) > 0:    
      qbe_output.add_msg('err: mms_cbbe: empty data found') 
      self.append_err_df(empty_data['MB_STAFFID'].drop_duplicates().rename('STAFFID'),"mms_cbbe: empty data found") 
    # check staffid pattern
    #select * from d where mb_term_dt>date() and (left(mb_staffid,2)<>'HF' or (mb_class='E' and substr(mb_staffid,9,2)<>'00') or (mb_class='S' and substr(mb_staffid,9,2)<>'01') or (mb_class='C' and !between(substr(mb_staffid,9,2),'02','99')))
    staffid_invalid = df[(df['MB_TERM_DT']> today)
                    &(
                      (df['MB_STAFFID'].str[:2] != 'HF')
                      | ((df['MB_CLASS']=='E') & (df['MB_STAFFID'].str[-2:]!='00'))
                      | ((df['MB_CLASS']=='S') & (df['MB_STAFFID'].str[-2:]!='01'))
                      | ((df['MB_CLASS']=='C') & ~((df['MB_STAFFID'].str[-2:-1].str.isnumeric()) & (df['MB_STAFFID'].str[-1:].str.isnumeric()) & (df['MB_STAFFID'].str[-2:]!='00') & (df['MB_STAFFID'].str[-2:]!='01')))
                    )
                    ]
    if len(staffid_invalid.index) > 0:    
      qbe_output.add_msg('err: mms_cbbe: staffid invalid found and output mms_cbbe_staffid_invalid.xslx') 
      self.append_err_df(staffid_invalid['MB_STAFFID'].drop_duplicates().rename('STAFFID'),"mms_cbbe: staffid invalid found") 
         
    mms_cbbe_result = len(empty_mb_term_date.index) == 0 & len(date_invalid.index) & len(empty_data.index) == 0 & len(staffid_invalid.index) == 0
    return mms_cbbe_result

  def check_actmb(self):
    #select * from a group by policy_no having count(*)>1  <-- check if any renew+ change maid in the same file
    df = self.actmb
    dup = df.groupby(['POLICY_NO']).count()
    duplicate = dup[dup['NAME']>1]
    #duplicate.iloc[[0]]
    #print(duplicate)
    if len(duplicate.index) > 0:
      list = df[df.set_index(['ID']).index.isin(duplicate.set_index(['ID']).index)]
      qbe_output.add_msg('err: actmb: renew+ change maid in the same file')
      self.append_err_df(list['ID'].drop_duplicates().rename('MB_POLICY'),"actmb: renew+ change maid in the same file")
 
    #select * from a where trans_type='CN' and old_ter_dt='?' <-- check if any change maid without old mail term date
    old_ter_dt = df[(df['TRANS_TYPE'] == 'CN') & (df['OLD_TER_DT'] == '?')]
    if len(old_ter_dt.index) > 0:
      qbe_output.add_msg('err: actmb: change maid without old mail term date') 
      self.append_err_df(old_ter_dt['TRANS_TYPE'].rename('MB_POLICY'),"actmb: change maid without old mail term date")

    #select * from a where ctod(expiry_dt)< ctod(com_dt)    <-- check if any maid's term date is earlier than join date and action <> "CP"
      #build two new df for compare their date
    df_expiry_header = settings['qbe']['data']['df_expiry']
    df_com_header = settings['qbe']['data']['df_com']
    df_expiry = pd.DataFrame(columns = df_expiry_header)
    df_com = pd.DataFrame(columns = df_com_header)
      #change date from datetype to str
    df_expiry = df['EXPIRY_DT'].astype(str)
    df_com = df['COM_DT'].astype(str)
      #build new df for output result
    df_compare = pd.DataFrame(columns = ['index','Date'])
      #if -> ctod(expiry_dt)< ctod(com_dt), then append the expiry date to new df named df_compare
    for i in range (len(df_com)):
      if df_expiry[i]<df_com[i]:
        #add loop index and expiry date into df_compare
        add = pd.Series({'index': (i), 'Date': df_expiry[i]})
        df_compare = df_compare.append(add,ignore_index=True)

    if len(df_compare.index) > 0:
      qbe_output.add_msg('err: actmb: maid s term date is earlier than join date and action <> "CP"') 
      self.append_err_df(df_compare['EXPIRY_DT'].rename('MB_POLICY'),"actmb: maid s term date is earlier than join date and action <> CP")

    #Check if any NP member already exists in MMS
    #select * from a inner join b on alltrim(a.policy_no)+alltrim(a.mbship_no)+(com_dt) ==alltrim(b.mb_policy)+alltrim(b.mb_cert_no) +dtoc(mb_jo_dt)  order by trans_type, policy_no
    df1 = self.actmb
    df2 = self.mms_cbbe
    # MBSHIP_NO from df1 is int type -> object, for merge
    df1['MBSHIP_NO'] = df1['MBSHIP_NO'].astype(str)
    join_result = df1.merge(df2, how='inner', left_on=['POLICY_NO','MBSHIP_NO','COM_DT'],
                                 right_on=['MB_POLICY','MB_CERT_NO','MB_JO_DT']  )
    result = join_result.sort_values(['TRANS_TYPE', 'POLICY_NO'])
    if len(result.index) > 0:    
      qbe_output.add_msg('err: actmb: NP member already exists in MMS') 
      self.append_err_df(result['POLICY_NO'].drop_duplicates().rename('MB_POLICY'),"actn has NP member already exists in MMS")

    #actmb validation check end, return all result
    actmb_result = False
    if len(duplicate.index) == 0 & len(old_ter_dt.index) == 0 & len(df_compare.index) == 0 & len(result.index) == 0:
      actmb_result = True
      return actmb_result
    else:
      return actmb_result

  def output_info(self):
    #print("user friendly log output")
    #select trans_type,count(*) from a group by trans_type
    qbe_output.actmb_actn = (self.actmb).groupby('TRANS_TYPE')['POLICY_NO'].count().reset_index(name="COUNT").T.to_dict().values()

    #pec


  def output_update(self):
    #use 22f alias a
    df_a = self.twotwof
    #use mb_cbbe alias b
    df_b = self.mms_cbbe
    #select * from a inner join b on a.mb_policy+a.mb_staffid==b.mb_policy+b.mb_staffid where a.mb_jo_dt<=b.mb_term_dt
    df_b['MB_TERM_DT'] = pd.to_datetime(df_b['MB_TERM_DT'], errors='coerce').dt.strftime('%d/%m/%Y')
    twotwof_mms_cbbe_result = df_a.merge(df_b, how='inner', left_on=['MB_POLICY','MB_STAFFID'], right_on=['MB_POLICY','MB_STAFFID'])
    twotwof_mms_cbbe_result_final = pd.DataFrame(columns=twotwof_mms_cbbe_result.columns, index=twotwof_mms_cbbe_result.index)
    twotwof_mms_cbbe_result_final = twotwof_mms_cbbe_result_final[0:0]
    k = 0
    for j in range (len(twotwof_mms_cbbe_result)):
      d1, m1, y1 = [int(x) for x in twotwof_mms_cbbe_result['MB_JO_DT_x'][j].split('/')]
      d2, m2, y2 = [int(x) for x in twotwof_mms_cbbe_result['MB_TERM_DT_y'][j].split('/')]
      b1 = date(y1, m1, d1)
      b2 = date(y2, m2, d2)
      if (b1 <= b2):
        twotwof_mms_cbbe_result_final.loc[k] = twotwof_mms_cbbe_result.iloc[j]
        k += 1
      else:continue
    #copy to 22g.dbf type fox2x
    twotwog = twotwof_mms_cbbe_result_final
    self.twotwog = twotwog
    #use 22g alias a
    df_a = self.twotwog
    #select * from a where mb_name_a<>mb_name_b or nw_co_cde_<>nw_co_cde2 or left(nw_sch_cde,4)<>nw_sch_cd2 
    # or mb_class_a<>mb_class_b or mb_jo_dt_a<>mb_jo_dt_b or mb_term_dt<>mb_term_d2 or mb_ter_a<>mb_ter_b 
    # or mb_id_a<>mb_id_b or mb_cert_no<>mb_cert_n2 or mb_staffid<>mb_staffi2 or mb_plan_a<>mb_plan_b 
    # or mb_policy_<>mb_policy2 or mb_dept_a<>mb_dept_b or mb_tel_a<>mb_tel_b or mb_bir_dt_ <> mb_bir_dt2
    twotwog_check_1 = df_a[(df_a['MB_NAME_x'] != df_a['MB_NAME_y']) | 
                            (df_a['NW_CO_CDE_x'] != df_a['NW_CO_CDE_y']) | 
                            (df_a['NW_SCH_CDE_x'].str[:4] != df_a['NW_SCH_CDE_y']) |
                            (df_a['MB_CLASS_x'] != df_a['MB_CLASS_y']) |
                            (df_a['MB_JO_DT_x'] != df_a['MB_JO_DT_y']) |
                            (df_a['MB_TERM_DT_x'] != df_a['MB_TERM_DT_y']) |
                            (df_a['MB_TER_x'] != df_a['MB_TER_y']) |
                            (df_a['MB_ID_x'] != df_a['MB_ID_y']) |
                            (df_a['MB_CERT_NO_x'] != df_a['MB_CERT_NO_y']) |
                            (df_a['MB_STAFFID'] != df_a['ID']) |
                            (df_a['MB_PLAN_x'] != df_a['MB_PLAN_y']) |
                            (df_a['MB_POLICY'] != df_a['POLICY_NO']) |
                            (df_a['MB_DEPT_x'] != df_a['MB_DEPT_y']) |
                            (df_a['MB_TEL_x'] != df_a['MB_TEL_y']) |
                            (df_a['MB_BIR_DT_x'] != df_a['MB_BIR_DT_y'])]
    if len(twotwog_check_1.index) > 0:
      qbe_output.add_msg('err: update: twotwog_check_1 failed')
      self.append_err_df(df_a['MB_NAME_x'],"update: twotwog_check_1 failed")
    #select * from a where mb_name_a<>mb_name_b or nw_co_cde_<>nw_co_cde2 or left(nw_sch_cde,4)<>nw_sch_cd2 
    # or mb_class_a<>mb_class_b or mb_ter_a<>mb_ter_b or mb_id_a<>mb_id_b or mb_cert_no<>mb_cert_n2
    # or mb_staffid<>mb_staffi2 or mb_plan_a<>mb_plan_b or mb_policy_<>mb_policy2 or mb_tel_a<>mb_tel_b or mb_bir_dt_ <> mb_bir_dt2
    twotwog_check_2 = df_a[(df_a['MB_NAME_x'] != df_a['MB_NAME_y']) | 
                            (df_a['NW_CO_CDE_x'] != df_a['NW_CO_CDE_y']) | 
                            (df_a['NW_SCH_CDE_x'].str[:4] != df_a['NW_SCH_CDE_y']) |
                            (df_a['MB_CLASS_x'] != df_a['MB_CLASS_y']) |
                            (df_a['MB_TER_x'] != df_a['MB_TER_y']) |
                            (df_a['MB_ID_x'] != df_a['MB_ID_y']) |
                            (df_a['MB_CERT_NO_x'] != df_a['MB_CERT_NO_y']) |
                            (df_a['MB_STAFFID'] != df_a['ID']) |
                            (df_a['MB_PLAN_x'] != df_a['MB_PLAN_y']) |
                            (df_a['MB_POLICY'] != df_a['POLICY_NO']) |
                            (df_a['MB_TEL_x'] != df_a['MB_TEL_y']) |
                            (df_a['MB_BIR_DT_x'] != df_a['MB_BIR_DT_y'])]
    if len(twotwog_check_2.index) > 0:
      qbe_output.add_msg('err: update: twotwog_check_2 failed')
      self.append_err_df(df_a['MB_NAME_x'],"update: twotwog_check_2 failed")
    #select * from a where mb_name_a<>mb_name_b
    twotwog_check_3 = df_a[(df_a['MB_NAME_x'] != df_a['MB_NAME_y'])]
    if len(twotwog_check_3.index) > 0:
      qbe_output.add_msg('err: update: twotwog_check_3 failed')
      self.append_err_df(df_a['MB_NAME_x'],"update: twotwog_check_3 failed")

    print ("test")

  def output_upload(self):
    #new is from set_mixed()
    new = self.new
    mixed = self.mixed
    upload_df = pd.DataFrame( columns =settings['data']['upload_header'] )
    twoyr_header = settings['qbe']['data']['2yr_header']
    twoyr = pd.DataFrame(columns = twoyr_header)
    #** check if any 2-yr member**
    #select  * from a where mb_term_Dt-mb_jo_dt>365
    k = 0
    for i in range (len(new)):
      end_date = datetime.strptime(new["MB_TERM_DT"][i], '%d/%m/%Y').date()
      start_date = datetime.strptime(new["MB_JO_DT"][i], '%d/%m/%Y').date()
      res = int(str((end_date - start_date).days))
      if res > 365:
        #copy to 2yr.dbf type fox2x
        twoyr.loc[k] = new.iloc[i]
        k += 1
        #update a set mb_term_dt=ctod(left(dtoc(mb_term_dt),6)+alltrim(str(year(mb_term_dt)-1))) where mb_term_Dt-mb_jo_dt>365
        new.loc[i , 'MB_TERM_DT'] = ((datetime.strptime(new["MB_TERM_DT"][i], '%d/%m/%Y').date()) - (pd.offsets.DateOffset(years=1)))
        new.loc[i , 'MB_TERM_DT'] = datetime.strptime(str(new['MB_TERM_DT'][i]), '%Y-%m-%d %H:%M:%S').strftime('%d/%m/%Y')
      else:continue
    #Update a set mb_dept=dtoc(mb_term_dt)
    new['MB_DEPT'] = new['MB_TERM_DT']
    #select a
    #print (new)

    #use 2yr alias a
    #update a set mb_jo_dt=ctod(left(dtoc(mb_term_dt),6)+alltrim(str(year(mb_term_dt)-1)))+1
    for i in range (len(twoyr)):
      twoyr.loc[i , 'MB_JO_DT'] = ((datetime.strptime((twoyr["MB_TERM_DT"][i]), '%d/%m/%Y').date()) - (pd.offsets.DateOffset(years=1))+ pd.offsets.DateOffset(days=1))
      twoyr.loc[i , 'MB_JO_DT'] = datetime.strptime(str(twoyr['MB_JO_DT'][i]), '%Y-%m-%d %H:%M:%S').strftime('%d/%m/%Y')

    #use member alias a
    #append from new    #append from 2yr
    member_header = settings['qbe']['data']['member_header']
    member = pd.DataFrame(columns = member_header)
    member = pd.concat([new, twoyr], ignore_index=True)
    #print (member)

    #use mb_cbbe alias b
    df_b = self.mms_cbbe
    df_a = member
    #select * from a inner join b on alltrim(a.mb_policy)+alltrim(a.mb_cert_no)+dtos(a.mb_jo_Dt)
    # ==alltrim(b.mb_policy)+alltrim(b.mb_cert_no)+dtos(b.mb_jo_dt)
    df_b['MB_JO_DT'] = df_b['MB_JO_DT'].apply(lambda x: x.strftime('%d/%m/%Y'))
    member_mms_cbbe_join_result = df_a.merge(df_b, how = 'inner', left_on=['MB_POLICY','MB_CERT_NO','MB_JO_DT'],
                                right_on= ['MB_POLICY','MB_CERT_NO','MB_JO_DT'])
    #print (member_mms_cbbe_join_result)
    if len(member_mms_cbbe_join_result.index) > 0:    
      qbe_output.add_msg('err: select * from a inner join b on alltrim(a.mb_policy)+alltrim(a.mb_cert_no)+dtos(a.mb_jo_Dt)==alltrim(b.mb_policy)+alltrim(b.mb_cert_no)+dtos(b.mb_jo_dt)') 
      self.append_err_df(member_mms_cbbe_join_result['MB_POLICY'].drop_duplicates().rename('MB_POLICY'),"select * from a inner join b on alltrim(a.mb_policy)+alltrim(a.mb_cert_no)+dtos(a.mb_jo_Dt)==alltrim(b.mb_policy)+alltrim(b.mb_cert_no)+dtos(b.mb_jo_dt)")

    #use upload_cbbe alias a
    upload_cbbe_header = settings['qbe']['data']['upload_cbbe_header']
    upload_cbbe = pd.DataFrame(columns = upload_cbbe_header)
    #append from member
    upload_cbbe = upload_cbbe.append(member)
    #replace all ser_type with 'MEO', NW_SCH_CDE with left(NW_SCH_CDE,4),PRECON_XDT with '31/12/2050', MB_IS_VIP with 'N'
    upload_cbbe.SER_TYPE = "MEO"
    upload_cbbe['NW_SCH_CDE'] = upload_cbbe['NW_SCH_CDE'].str.slice(stop=4)
    upload_cbbe.PRECON_XDT = '31/12/2050'
    upload_cbbe.MB_IS_VIP = "N"
    #use mb_cbbe alias b
    df_b = self.mms_cbbe
    df_a = upload_cbbe
    #select * from a inner join b on alltrim(a.mb_policy)+alltrim(a.mb_cert_no)+dtos(a.mb_jo_Dt)
    # ==alltrim(b.mb_policy)+alltrim(b.mb_cert_no)+dtos(b.mb_jo_dt)
    upload_cbbe_mms_cbbe_join_result = df_a.merge(df_b, how = 'inner', left_on=['MB_POLICY','MB_CERT_NO','MB_JO_DT'],
                                right_on= ['MB_POLICY','MB_CERT_NO','MB_JO_DT'])
    print (upload_cbbe_mms_cbbe_join_result)
    if len(upload_cbbe_mms_cbbe_join_result.index) == 0:    
      qbe_output.add_msg('err: select * from a inner join b on alltrim(a.mb_policy)+alltrim(a.mb_cert_no)+dtos(a.mb_jo_Dt)==alltrim(b.mb_policy)+alltrim(b.mb_cert_no)+dtos(b.mb_jo_dt)') 
      self.append_err_df(upload_cbbe_mms_cbbe_join_result['MB_POLICY'].drop_duplicates().rename('MB_POLICY'),"select * from a inner join b on alltrim(a.mb_policy)+alltrim(a.mb_cert_no)+dtos(a.mb_jo_Dt)==alltrim(b.mb_policy)+alltrim(b.mb_cert_no)+dtos(b.mb_jo_dt)")
     

  def output_err_file(self):
    if len( self.err_df.index)>0:
      qbe_output.add_msg("err: validation failure and output err_df.xslx'")
      self.err_df.to_excel(self.data_output_path+"err_df.xlsx", sheet_name = 'Sheet1', index=False, engine='xlsxwriter')
   

  def output_file(self):
    print("add function for output files")
    self.output_upload()
    self.output_update()

      
  def set_temp_df(self):
    print("optional: temp df during programming")
     
 

if __name__ == '__main__':
  pd.set_option('display.max_columns', None)
  # program start  
  # create logger
  qbe_logger = logging.getLogger('dev.qbe')

  # tag
  tag = '[qbe]'
  today = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
  qbe_output = qbeOutput(datetime.now())   

  try:
     
    qbe_local = qbe( sys.argv[1], qbe_output) 
    if not os.path.exists(qbe_local.data_output_path):
        os.makedirs(qbe_local.data_output_path)
    qbe_local.validate()
    qbe_local.set_temp_df()
    qbe_local.output_info()
    qbe_local.output_file()
    


  except:  
    qbe_logger.exception("message")
    qbe_output.add_msg("exception found")
  finally:    
    qbe_output.end_time=datetime.now()
    #qbe_local.output_err_file()

  # test after exception
  #tempa = qbeOutputToTemplate(qbe_output)
  #with open(settings['qbe']['path']['template'], 'r') as f:
      #src = Template(f.read())
      #result = src.substitute(tempa.__dict__)
     # print(result,  file=open(settings['qbe']['path']['output_data'] + sys.argv[1][:4] + '/' + sys.argv[1][4:6] + '/' + sys.argv[1][6:] + '/'+'\log.txt', 'a'))
