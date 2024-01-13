import pandas as pd
import numpy as np
import pyodbc
from pyodbc import Connection, Cursor
from typing import List,Dict,Any,Union,Optional
import os
from enum import Enum
from decimal import Decimal
from datetime import datetime

wild_card = str.maketrans({'*':'%'})

class Error(Enum):
    """エラーコード"""        
    NO_ERR = 0
    """エラーなし"""    
    FILE_TYPE_ERR = 1
    """ファイルタイプエラー"""    
    UNDEFINED_DATA_TYPE = 2
    """未定義のデータタイプ"""
    NO_DATA_IN_TABLE = 3
    """テーブルにデータがありません"""
    INVALID_INPUT = 4
    """無効な入力"""
    DATA_TYPE_MISMATCH = 5
    """データ型の不整合"""
    INVALID_COLUMN_NAME = 6
    """無効な列名"""
    NO_ROW_EXIST = 7
    """行が存在しない"""
    
class SerchCondition(Enum):
    """検索条件"""
    Exact = 0
    """完全一致、等しい、Boolでも適用可能"""
    StartWith = 1
    """~で始まる"""    
    EndWith = 2
    """~で終わる"""    
    Contains = 3
    """~を含む"""
    SmallerThan = 4
    """~より小さい"""
    OrSmallerThan = 5 
    """~以下"""
    LargerThan = 6
    """~より大きい"""
    OrLargerThan = 7 
    """~以上"""    
    
class DataRowState(Enum):
    """データフレームの行の状態"""
    NotChange = 0
    """ 変化なし """
    Updated = 1
    """ 更新されている """
    Added = 2
    """ 追加されている """
    Deleted = 3
    """ 削除されている """

class DataBaseCtrl():
    """データベース(.accdb)制御クラス
    """
    Int_DF:pd.DataFrame = None
    """クラス内部データフレーム"""
    Column_DF:pd.DataFrame = None
    """クラス内部列情報データフレーム"""
    RowState_DF:pd.DataFrame = None
    """クラス内部データフレームの行状態"""
    TableName:str
    """テーブル名"""
    DirectMode:bool
    """直接データベースアクセスモード"""
    strCon:str
    """接続文字列"""
    conn:Connection = None
    """接続オブジェクト"""
    cursor:Cursor = None
    """データベースカーソル"""
    err:Error
    """エラーコード""" 
    
    def __init__(self, DataBase_Path:str, TableName:str, DirectMode:bool=False) -> None:
        """データベース(.accdb)制御クラス(コンストラクター)

        Args:
            DataBase_Path (str): データベースファイルパス
            TableName (str): テーブル名
            DirectMode (bool, optional): 直接データベースアクセスモード=True. Defaults to False.
        """
        self.TableName = TableName
        self.DirectMode = DirectMode
        #拡張子の判定
        file_name = os.path.basename(DataBase_Path)
        file_ext = os.path.splitext(file_name)[1]
        if(file_ext != '.accdb'):
            self.err = Error.FILE_TYPE_ERR
            return                
        #SQLの作成
        self.strCon = 'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
        self.strCon += f'DBQ={DataBase_Path};'
        #接続
        self.conn = pyodbc.connect(self.strCon)
        self.cursor = self.conn.cursor()
        
    def __del__(self) -> None:
        """デストラクタ"""
        #閉じる
        if(self.cursor != None):
            self.cursor.close()
        if(self.conn != None):
            self.conn.close()
            
    def UpdateInternalDataFrame(self, set_index:Optional[str]='ID') -> bool:
        """データベースから内部データフレームを更新する。

        Args:
            set_index (Optional[str], optional): インデクスにする行名, Noneとするとインデックス指定しない. Defaults to 'ID'.

        Returns:
            bool: 成功=True / 失敗=False
        """        
        #SQLでデータベースの読み取り
        sql = self.__SelectSQL()        
        self.cursor.execute(sql)
        res = self.cursor.fetchall()
        if(len(res)<1):
            self.err = Error.NO_DATA_IN_TABLE
            return False
        #行データの作成
        column_inf = res[0].cursor_description       
        self.Column_DF = pd.DataFrame(column_inf,None,['Name','Type','N/A','Size','Tol','Scale','Flag'])
        #データフレーム構築
        df = pd.DataFrame(np.array(res), columns=[c for c in self.Column_DF['Name']])
        if(type(set_index) == str):
            self.Int_DF = df.set_index(set_index)
        #データ行の状態データフレーム構築、イニシャライズ
        data_dict:Dict[str,List[Any]]={}                
        data_dict['ID'] = self.Int_DF.index.to_list()
        data_dict['RowState'] = [DataRowState.NotChange for idx in self.Int_DF.index.to_list()] 
        df = pd.DataFrame(data_dict,)
        if(type(set_index) == str):
            self.RowState_DF = df.set_index(set_index)       
            
        self.err = Error.NO_ERR
        return True        
    
    def GetCopyInternalDataFrame(self) -> pd.DataFrame:
        """内部データフレームのコピーを取得する。

        Returns:
            pd.DataFrame: 内部データフレームのコピー
        
        Remarks:
            行状態が削除のものはコピーされない
        """
        serch_ser = self.RowState_DF['RowState'] != DataRowState.Deleted        
        return self.Int_DF[serch_ser]
    
    def SelectRowByID(self, ID:Union[int,str], Ext_DF:pd.DataFrame=None) -> pd.DataFrame:
        """IDでデータフレームの行を検索（IDがKEYインデクスになっている場合）

        Args:
            ID (int, str): ID
            Ext_DF (pd.DataFrame, optional): 検索する外部データフレーム、Noneで内部データフレーム. Defaults to None.

        Returns:
            pd.DataFrame: 検索結果
        """        
        if(type(Ext_DF) == type(None)):
            Selected_DB = self.Int_DF           
        else:
            Selected_DB = Ext_DF
        sel_ser = Selected_DB.index == ID
        out_df:pd.DataFrame = Selected_DB[sel_ser]        
        return out_df
    
    def SerchRows(self, SerchDict:Dict[str,Union[str,int,float,Decimal,bool]],
                  Serch_condition:SerchCondition=SerchCondition.Exact,
                  MultiSerch_Type:bool=True,
                  Ext_DF:pd.DataFrame=None) -> pd.DataFrame:
        """検索条件で検索する。

        Args:
            SerchDict (Dict[str,Union[str,int,float,Decimal,bool]]): 検索内容<列名,値>
            Serch_condition (SerchCondition, optional):検索条件. Defaults to SerchCondition.Exact.
            MultiSerch_Type (bool, optional): 検索Dictが複数の場合、AND検索=>True / OR検索=>False. Defaults to True.
            Ext_DF (pd.DataFrame, optional): 検索する外部データフレーム、Noneで内部データフレーム. Defaults to None.

        Returns:
            pd.DataFrame: 検索結果
            
        Remarks:
            検索内容は同じ列名(Key)で複数条件はできません。絞り込み検索は、一度出た結果を外部データフレームとして検索してください。        
        """      
        #検索するデータフレーム       
        if(type(Ext_DF) == type(None)):
            df = self.Int_DF
        elif(type(Ext_DF) == type(pd.DataFrame()) and not(Ext_DF.empty)):
            df = Ext_DF
        else:
            self.err = Error.INVALID_INPUT
            return pd.DataFrame() # Empty DataFrame
        
        #検索
        SerchSeries:pd.Series[bool] = pd.Series()       
        for key in SerchDict:            
            serch:pd.Series[bool] = pd.Series()
            if(Serch_condition == SerchCondition.Exact): #完全一致
                serch = df[key] == SerchDict[key]
            elif(Serch_condition == SerchCondition.StartWith): #～で始まる
                if(type(SerchDict[key]) == str):
                    serch = df[key].str.startswith(SerchDict[key])
            elif(Serch_condition == SerchCondition.EndWith): #～で終わる
                if(type(SerchDict[key]) == str):
                    serch = df[key].str.endswith(SerchDict[key])
            elif(Serch_condition == SerchCondition.Contains): #～を含む
                if(type(SerchDict[key]) == str):
                    serch = df[key].str.contains(SerchDict[key])
            elif(Serch_condition == SerchCondition.SmallerThan): #~より小さい
                if(type(SerchDict[key]) == int or type(SerchDict[key]) == float or type(SerchDict[key]) == Decimal):
                    serch = df[key] < SerchDict[key]
            elif(Serch_condition == SerchCondition.OrSmallerThan): #~以下
                if(type(SerchDict[key]) == int or type(SerchDict[key]) == float or type(SerchDict[key]) == Decimal):
                    serch = df[key] <= SerchDict[key]
            elif(Serch_condition == SerchCondition.LargerThan): #~より大きい
                if(type(SerchDict[key]) == int or type(SerchDict[key]) == float or type(SerchDict[key]) == Decimal):
                    serch = df[key] > SerchDict[key]
            elif(Serch_condition == SerchCondition.OrLargerThan): #~以上
                if(type(SerchDict[key]) == int or type(SerchDict[key]) == float or type(SerchDict[key]) == Decimal):
                    serch = df[key] >= SerchDict[key]
            
            #検索用Series[bool]合成演算
            if(SerchSeries.empty and not(serch.empty)):
              SerchSeries = serch
            elif(not(SerchSeries.empty) and not(serch.empty)):
                if(MultiSerch_Type):    
                    SerchSeries = SerchSeries & serch
                else:
                    SerchSeries = SerchSeries | serch
                    
        return df[SerchSeries]
    
    def UpdateRow_IntDataFrame(self, ID:Union[int,str], UpdateDict:Dict[str,Any]) -> bool:
        """内部データフレームの行を更新（変更）する。

        Args:
            ID (Union[int,str]): 変更する行のID
            UpdateDict (Dict[str,Any]): 変更する内容<列名,変更後の値>
            
        Returns:
            bool: 成功=True / 失敗=False
            
        Remarks:
            データベースは、内部データフレームでデータベースを更新（同期）させるまで変更されない。
        """
        #IDがIndexとなる行が存在するか確認        
        if(self.Int_DF[self.Int_DF.index == ID].empty):
            self.err = Error.NO_ROW_EXIST
            return False
        #行の更新        
        for key in UpdateDict:             
            ValueType = self.Column_DF[self.Column_DF['Name'] == key]['Type']
            if(ValueType.empty):
                self.err = Error.INVALID_COLUMN_NAME
                return False
            if(type(UpdateDict[key]) != ValueType.values[0]):
                self.err = Error.DATA_TYPE_MISMATCH
                return False
            #行の状態更新
            if(self.RowState_DF.at[ID,'RowState'] == DataRowState.NotChange):
                self.Int_DF.at[ID,key] = UpdateDict[key]
                self.RowState_DF.at[ID,'RowState'] = DataRowState.Updated
            elif(self.RowState_DF.at[ID,'RowState'] == DataRowState.Updated):
                self.Int_DF.at[ID,key] = UpdateDict[key]
                self.RowState_DF.at[ID,'RowState'] = DataRowState.Added
            elif(self.RowState_DF.at[ID,'RowState'] == DataRowState.Added):
                self.Int_DF.at[ID,key] = UpdateDict[key]
            elif(self.RowState_DF.at[ID,'RowState'] == DataRowState.Deleted):
                pass
            
        return True
            
    def AddRow_IntDataFrame(self, AddDict:Dict[str,Any],ID:Union[int,str]=None) -> bool:
        """内部データフレームに行を追加する。

        Args:
            AddDict (Dict[str,Any]): 追加する内容<列名,値>
            ID (Union[int,str], optional): ID、Noneで自動取得. Defaults to None.

        Returns:
            bool: 成功=True / 失敗=False
        
        Remarks:
            データベースは、内部データフレームでデータベースを更新（同期）させるまで変更されない。
        """
        #データ型の確認
        for key in AddDict:
            ValueType = self.Column_DF[self.Column_DF['Name'] == key]['Type']
            if(ValueType.empty):
                self.err = Error.INVALID_COLUMN_NAME
                return False
            if(type(AddDict[key]) != ValueType.values[0]):
                self.err = Error.DATA_TYPE_MISMATCH
                return False
        #行の追加
        if(type(ID) == type(None)):
            new_id:Union[int,str] = self.Int_DF.index.max() + 1
        else:
            new_id = ID 
        new_row = pd.DataFrame([AddDict],index=[new_id])
        self.Int_DF = pd.concat([self.Int_DF,new_row])
        self.RowState_DF.at[new_id,'RowState'] = DataRowState.Added
        
        return True
    
    def DeleteRow_IntDataFrame(self, ID:Union[int,str], Del:bool=True) -> bool:
        """内部データフレームの行を削除する。(RowStateのみ変更)

        Args:
            ID (int, str): 削除する行ID
            Del (bool, optional): 削除=True / 削除を解除=False. Defaults to True.

        Returns:
            bool: 成功=True / 失敗=False
            
        Remarks:
            データベースは、内部データフレームでデータベースを更新（同期）させるまで変更されない。\n
            変更・追加した行は削除できない。一度データベースと同期をとった後削除してください。
        """
        #IDがIndexとなる行が存在するか確認        
        if(self.Int_DF.loc[ID].empty):
            self.err = Error.NO_ROW_EXIST
            return False
        #行状態の変更
        if(Del):
            if(self.RowState_DF.at[ID,'RowState'] == DataRowState.NotChange):
                self.RowState_DF.at[ID,'RowState'] = DataRowState.Deleted
            elif(self.RowState_DF.at[ID,'RowState'] == DataRowState.Updated):
                pass #アップデートした行は削除できない。一度データベースと同期をとってから削除してください
            elif(self.RowState_DF.at[ID,'RowState'] == DataRowState.Added):
                pass #追加した行は削除できない。一度データベースと同期をとってから削除してください
            elif(self.RowState_DF.at[ID,'RowState'] == DataRowState.Deleted):
                pass
        else:
            if(self.RowState_DF.at[ID,'RowState'] == DataRowState.Deleted):
                self.RowState_DF.at[ID,'RowState'] = DataRowState.NotChange
        return True
    
    def UpdateDataBase(self) -> bool:
        """データベースを内部DataFrameで更新する（同期）。

        Returns:
            bool: 成功=True / 失敗=False
        """        
        sql_list:List[str] =[]
        #UpdateのSQL
        update_rows_ser = self.RowState_DF['RowState'] == DataRowState.Updated        
        updated_df = self.Int_DF[update_rows_ser]
        if(not(updated_df.empty)):
            sql_list.extend(self.__UpdateSQL(updated_df))        
        #InsertのSQL
        insert_rows_ser = self.RowState_DF['RowState'] == DataRowState.Added
        insert_df = self.Int_DF[insert_rows_ser]
        if(not(insert_df.empty)):
            sql_list.extend(self.__InsertSQL(insert_df))
        #DeleteのSQL
        delete_rows_ser = self.RowState_DF['RowState'] == DataRowState.Deleted
        delete_df = self.Int_DF[delete_rows_ser]
        if(not(delete_df.empty)):
            sql_list.extend(self.__DeleteSQL(delete_df))            
        #SQLの実行
        for sql in sql_list:
            self.cursor.execute(sql)
        self.conn.commit()
        self.UpdateInternalDataFrame()
        return True
    
    def __SelectSQL(self, Data:Dict[str,Any]=None) -> str:
        """SELECTのSQL

        Args:
            Data (Dict[str,Any], optional): 検索データ / Noneで全データ. Defaults to None.

        Returns:
            str: SQLコマンド文字列
            
        Remarks:
            Data:検索データが文字列で*を含む場合は曖昧検索になる
        """        
        sql_str = f'SELECT * FROM [{self.TableName}]'
        if(type(Data) == type(None)):
            return sql_str
        elif(type(Data) != dict):
            self.err = Error.INVALID_INPUT
            return ''
               
        sql_str += ' WHERE'
        for i,key in enumerate(Data):
            if(not(key in self.Column_DF['Name'].to_list())):
                self.err = Error.INVALID_COLUMN_NAME
                return ''
            if(i==0):
                if(type(Data[key]) == int and self.Column_DF[self.Column_DF['Name']==key]['Type'].to_list()[0] == int):
                    sql_str += f' {key} = {Data[key]}'
                elif(type(Data[key]) == float and self.Column_DF[self.Column_DF['Name']==key]['Type'].to_list()[0] == float):
                    sql_str += f' {key} = {Data[key]}'
                elif(type(Data[key]) == Decimal and self.Column_DF[self.Column_DF['Name']==key]['Type'].to_list()[0] == Decimal):
                    sql_str += f' {key} = {Data[key]}'
                elif(type(Data[key]) == str and self.Column_DF[self.Column_DF['Name']==key]['Type'].to_list()[0] == str):
                    if(str(Data[key]).find('*')>=0):                        
                        sql_str += f' {key} LIKE \'{str(Data[key]).translate(wild_card)}\''
                    else:
                        sql_str += f' {key} = \'{Data[key]}\''
                elif(type(Data[key]) == bool and self.Column_DF[self.Column_DF['Name']==key]['Type'].to_list()[0] == bool):
                    if(Data[key]):
                        sql_str += f' {key} = 1'
                    else:
                        sql_str += f' {key} = 0'
                elif(type(Data[key]) == datetime and self.Column_DF[self.Column_DF['Name']==key]['Type'].to_list()[0] == datetime):
                    sql_str += f' {key} = \'{datetime(Data[key]).strftime("%Y-%m-%d %H:%M:%S")}\''
                else:
                    self.err = Error.DATA_TYPE_MISMATCH
                    return ''
            else:
                if(type(Data[key]) == int and self.Column_DF[self.Column_DF['Name']==key]['Type'].to_list()[0] == int):
                    sql_str += f' AND {key} = {Data[key]}'
                elif(type(Data[key]) == float and self.Column_DF[self.Column_DF['Name']==key]['Type'].to_list()[0] == float):
                    sql_str += f' AND {key} = {Data[key]}'
                elif(type(Data[key]) == Decimal and self.Column_DF[self.Column_DF['Name']==key]['Type'].to_list()[0] == Decimal):
                    sql_str += f' AND {key} = {Data[key]}'
                elif(type(Data[key]) == str and self.Column_DF[self.Column_DF['Name']==key]['Type'].to_list()[0] == str):
                    if(str(Data[key]).find('*')>=0):                        
                        sql_str += f' AND {key} LIKE \'{str(Data[key]).translate(wild_card)}\''
                    else:
                        sql_str += f' AND {key} = \'{Data[key]}\''
                elif(type(Data[key]) == bool and self.Column_DF[self.Column_DF['Name']==key]['Type'].to_list()[0] == bool):
                    if(Data[key]):
                        sql_str += f' AND {key} = 1'
                    else:
                        sql_str += f' AND {key} = 0'
                elif(type(Data[key]) == datetime and self.Column_DF[self.Column_DF['Name']==key]['Type'].to_list()[0] == datetime):
                    sql_str += f' AND {key} = \'{datetime(Data[key]).strftime("%Y-%m-%d %H:%M:%S")}\''
                else:
                    self.err = Error.DATA_TYPE_MISMATCH
                    return ''
        sql_str = sql_str + ';'
        return sql_str
        
    def __UpdateSQL(self, Data:pd.DataFrame) -> List[str]:
        """SELECTのSQL

        Args:
            Data (pd.DataFrame): UPDATEするデータ

        Returns:
            List[str]: SQLコマンド文字列リスト
        """
        out_str_list:List[str] = []      
        if(Data.empty):
            self.err = Error.INVALID_INPUT
            return []        
        for j,row in enumerate(Data.values.tolist()):
            sql_str = f'UPDATE [{self.TableName}] SET'
            for i,col in enumerate(self.Column_DF.values.tolist()):
                if(i>0):
                    if(type(row[i-1]) != type(None)):                    
                        if(col[1]==int):                        
                            sql_str += f' {col[0]} = {row[i-1]},'
                        elif(col[1]==float):
                            sql_str += f' {col[0]} = {row[i-1]},'
                        elif(col[1]==Decimal):
                            sql_str += f' {col[0]} = {row[i-1]},'
                        elif(col[1]==str):
                            sql_str += f' {col[0]} = \'{row[i-1]}\','
                        elif(col[1]==bool):
                            if(row[i-1]):
                                sql_str += f' {col[0]} = 1,'
                            else:
                                sql_str += f' {col[0]} = 0,'
                        elif(col[1]==datetime):
                            if(pd.notna(row[i-1])):                        
                                time_stamp:pd.Timestamp = row[i-1]                        
                                date_py = time_stamp.to_pydatetime()
                                sql_str += f' {col[0]} = \'{date_py.strftime("%Y-%m-%d")}\','
                        else:
                            self.err = Error.UNDEFINED_DATA_TYPE
                        
            id_val:Union[int,str] = Data.index.to_list()[j]
            if(type(id_val) == int):
                sql_str = sql_str[0:-1] + f' WHERE {self.Column_DF.values.tolist()[0][0]} = {id_val};'
            elif(type(id_val) == str):
                sql_str = sql_str[0:-1] + f' WHERE {self.Column_DF.values.tolist()[0][0]} = \'{id_val}\';'
            
            out_str_list.append(sql_str)
        return out_str_list
            
    def __InsertSQL(self, Data:pd.DataFrame) -> List[str]:
        """INSERTのSQL

        Args:
            Data (pd.DataFrame): Insertするデータ

        Returns:
            List[str]: SQLコマンド文字列リスト
        """
        blank_value='NULL'
        out_str_list:List[str] = []      
        if(Data.empty):
            self.err = Error.INVALID_INPUT
            return []
        for j,row in enumerate(Data.values.tolist()):
            sql_str = f'INSERT INTO [{self.TableName}]'
            sql_col = '('
            sql_val = 'VALUES ('
            for i,col in enumerate(self.Column_DF.values.tolist()):              
                if(i==0):
                    if(col[1]==int):
                        sql_col += f"{col[0]},"
                        sql_val += f"{Data.index[j]},"
                    elif(col[1]==str):
                        sql_col += f"{col[0]},"
                        sql_val += f"\'{Data.index[j]}\',"
                elif(i>0):
                    if(col[1]==int):
                        sql_col += f'{col[0]}, '
                        if(pd.notna(row[i-1])):
                            sql_val += f'{row[i-1]}, '                            
                        else:
                            sql_val += f'{blank_value}, '                            
                    elif(col[1]==float):
                        sql_col += f'{col[0]}, '
                        if(pd.notna(row[i-1])):
                            sql_val += f'{row[i-1]}, '                            
                        else:
                            sql_val += f'{blank_value}, '
                    elif(col[1]==Decimal):
                        sql_col += f'{col[0]}, '
                        if(pd.notna(row[i-1])):
                            sql_val += f'{row[i-1]}, '                            
                        else:
                            sql_val += f'{blank_value}, '
                    elif(col[1]==str):
                        sql_col += f'{col[0]}, '
                        if(pd.notna(row[i-1])):
                            sql_val += f'\'{row[i-1]}\', '                            
                        else:
                            sql_val += f'{blank_value}, '                            
                    elif(col[1]==bool):
                        if(not(pd.notna(row[i-1]))):
                            sql_col += f'{col[0]}, '
                            sql_val += f'{blank_value}, '
                        elif(row[i-1]):
                            sql_col += f'{col[0]}, '
                            sql_val += '1, '
                        elif(not(row[i-1])):
                            sql_col += f'{col[0]}, '
                            sql_val += '0, '
                    elif(col[1]==datetime):
                        sql_col += f'{col[0]}, '
                        if(pd.notna(row[i-1])):
                            sql_val += f'\'{datetime(row[i-1]).strftime("%Y-%m-%d %H:%M:%S")}\', '                            
                        else:
                            sql_val += f'{blank_value}, '
                    else:
                        self.err = Error.UNDEFINED_DATA_TYPE
            sql_col = sql_col[0:-2] + ')'
            sql_val = sql_val[0:-2] + ')'
            sql_str = f'{sql_str} {sql_col} {sql_val};'
            out_str_list.append(sql_str)
        return out_str_list
    
    def __DeleteSQL(self, Data:pd.DataFrame) -> List[str]:
        """DeleteのSQL

        Args:
            Data (pd.DataFrame): 削除するデータ

        Returns:
            List[str]: SQLコマンド文字列リスト
        """
        out_str_list:List[str] = []      
        if(Data.empty):
            self.err = Error.INVALID_INPUT
            return out_str_list
        for row_idx in Data.index.tolist():
            sql_str = f'DELETE FROM [{self.TableName}] '
            sql_str += 'WHERE '
            sql_str += f'{self.Column_DF.values[0][0]} = '
            if(type(row_idx)==int):
                sql_str += f'{row_idx};'
            elif(type(row_idx)==str):
                sql_str += f'\'{row_idx}\';'
            out_str_list.append(sql_str)
        
        return out_str_list                       
        
