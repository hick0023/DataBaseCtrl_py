import pandas as pd
import numpy as np
import pyodbc
from pyodbc import Connection, Cursor
from typing import List,Dict,Any,Union,Optional
import os
from enum import Enum
from decimal import Decimal
from datetime import datetime,date,time

wild_card = str.maketrans({'*':'%'})
"""Wilde Card Translate"""

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
    NOT_WORK_THIS_MODE = 8
    """設定されたモードでは動作しない"""
    SELECT_CONDITION_ERR = 9
    """SQLでSELECTの条件エラー"""
    DATA_NOT_UNIQUE_BY_ID = 10
    """データがIDに対して固有ではない"""
    
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

class AccessDataType(Enum):
    """Accessデータベース列のSQLデータ型
    """
    CHAR = 1
    """固定長の文字列,Parm(最大文字数)"""
    VARCHAR = 2
    """可変長の文字列,Parm(最大文字数)"""
    MEMO = 3
    """長いテキストやメモ"""
    BYTE = 4
    """1 バイトの符号なし整数"""
    INTEGER = 5
    """2 バイトの符号付き整数"""
    LONG = 6
    """4 バイトの符号付き整数"""
    SINGLE = 7
    """単精度浮動小数点数"""
    DOUBLE = 8
    """倍精度浮動小数点数"""
    CURRENCY = 9
    """通貨型"""
    DECIMAL = 10
    """【使用不能】 固定小数点数,parm(全体の桁数,小数点以下の桁)"""
    AUTOINCREMENT = 11
    """自動増分の整数"""
    DATE = 12
    """日付のみ"""
    TIME = 13
    """時刻のみ"""
    DATETIME = 14
    """日付と時刻"""
    TIMESTAMP = 15
    """タイムスタンプ"""
    YESNO = 16
    """ブール型 (Yes/No)"""
    OLEOBJECT = 17
    """OLE オブジェクト"""
    HYPERLINK = 18
    """【使用不能】ハイパーリンク"""
    GUID = 19
    """GUID (グローバル一意識別子)"""
    REAL = 20
    """SINGLE"""
    VARBINARY = 21
    """拡張Datetime"""
    
Access_dtype_py:Dict[AccessDataType,Optional[type]] = {
    AccessDataType.CHAR:str,
    AccessDataType.VARCHAR:str,
    AccessDataType.MEMO:str,
    AccessDataType.BYTE:int,
    AccessDataType.INTEGER:int,
    AccessDataType.LONG:int,
    AccessDataType.SINGLE:float,
    AccessDataType.DOUBLE:float,
    AccessDataType.CURRENCY:Decimal,
    AccessDataType.DECIMAL:None,
    AccessDataType.AUTOINCREMENT:int,
    AccessDataType.DATE:datetime,
    AccessDataType.TIME:datetime,
    AccessDataType.DATETIME:datetime,
    AccessDataType.TIMESTAMP:datetime,
    AccessDataType.YESNO:bool,
    AccessDataType.OLEOBJECT:bytearray,
    AccessDataType.HYPERLINK:None,
    AccessDataType.GUID:str,
    AccessDataType.REAL:float,
    AccessDataType.VARBINARY:bytearray
}
"""Access data type dict to python data type """

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
    col_inf_columns = [
        'table_cat',
        'table_schem',
        'table_name',
        'column_name',
        'data_type',
        'type_name',
        'column_size',
        'buffer_length',
        'decimal_digits',
        'num_prec_radix',
        'nullable',
        'remarks',
        'column_def',
        'sql_data_type',
        'sql_datetime_sub',
        'char_octet_length',
        'ordinal_position',
        'is_nullable',
        'ordinal']
    """Coulumns of Column DataFarme""" 
    
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
        #SQL接続文字列作成
        self.strCon = 'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
        self.strCon += f'DBQ={DataBase_Path};'        
        #接続
        self.conn = pyodbc.connect(self.strCon)
        # ODBCドライバーに送信する属性を指定する
        #attrs_before = {pyodbc.SQL_MAX_COLUMNS_IN_SELECT: 255}
        #self.conn.set_attr(pyodbc.SQL_MAX_COLUMNS_IN_SELECT,255)
        #self.conn = pyodbc.connect(self.strCon,attrs_before=attrs_before)
        self.cursor = self.conn.cursor()                    
        #列情報の取得
        if(self.IsTableExist()):
            self.__GetColumnNameFromDataBase()
        self.err = Error.NO_ERR
        
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
        #直接データベースアクセスモードでは動作しない
        if(self.DirectMode):
            self.err = Error.NOT_WORK_THIS_MODE
            return False
        #SQLでデータベースの読み取り
        sql = self.__SelectSQL()        
        self.cursor.execute(sql)
        res = self.cursor.fetchall()
        if(len(res)<1):
            self.err = Error.NO_DATA_IN_TABLE
            return False        
        #データフレーム構築
        df = self.__SqlResultToDataFrame(res,set_index)        
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
        #直接データベースアクセスモードでは動作しない
        if(self.DirectMode):
            self.err = Error.NOT_WORK_THIS_MODE
            return pd.DataFrame()
        #行StateがDeleted以外を返す。
        serch_ser = self.RowState_DF['RowState'] != DataRowState.Deleted        
        return self.Int_DF[serch_ser]
    
    def SelectRowByID(self, ID:Union[int,str,None], Ext_DF:pd.DataFrame=None) -> pd.DataFrame:
        """IDでデータフレームの行を検索（IDがKEYインデクスになっている場合）

        Args:
            ID (int, str, None): ID, "*" or Noneで全検索
            Ext_DF (pd.DataFrame, optional): 検索する外部データフレーム、Noneで内部データフレーム. Defaults to None.

        Returns:
            pd.DataFrame: 検索結果
        """
        out_df = pd.DataFrame()        
        if(self.DirectMode and type(Ext_DF) == type(None)):    #ダイレクトアクセスモードの場合
            if ID =="*":
                sql = self.__SelectSQL()
            else:
                sql = self.__SelectSQL({"ID":ID})
            self.cursor.execute(sql)
            res = self.cursor.fetchall()
            out_df = self.__SqlResultToDataFrame(res)
            out_df = out_df.replace([None],[float("nan")]).replace(["None"],[float("nan")])
        else: #クラス内データフレームモード            
            if(type(Ext_DF) == type(None)):
                Selected_DB = self.Int_DF           
            else:
                Selected_DB = Ext_DF
            sel_ser = Selected_DB.index == ID
            out_df = Selected_DB[sel_ser]        
        return out_df
    
    def SerchRows(self, SerchDict:Dict[str,Union[str,int,float,Decimal,bool]],
                  Serch_condition:SerchCondition=SerchCondition.Exact,
                  MultiSerch_Type:bool=True,
                  Ext_DF:pd.DataFrame=None) -> pd.DataFrame:
        """検索条件で行を検索する。

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
        out_df = pd.DataFrame()
        if(self.DirectMode and type(Ext_DF) == type(None)): #直接アクセスモード
            sql = self.__SelectSQL(SerchDict, Serch_condition)
            self.cursor.execute(sql)
            res = self.cursor.fetchall()
            out_df = self.__SqlResultToDataFrame(res)
        else: #クラス内データフレームモード       
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
            out_df = df[SerchSeries]
                    
        return out_df   
    
    def UpdateRow(self, ID:Union[int,str], UpdateDict:Dict[str,Any]) -> bool:
        """内部データフレームまたはデータベースの行を更新（変更）する。

        Args:
            ID (Union[int,str]): 変更する行のID
            UpdateDict (Dict[str,Any]): 変更する内容<列名,変更後の値>
            
        Returns:
            bool: 成功=True / 失敗=False
            
        Remarks:
            データフレームモード: 内部データフレームが更新、データベースを更新（同期）させるまで変更されない。UpdateDataBase()
            ダイレクトモード: データベースが直接更新される。
        """
        ret_bool:bool
        if(self.DirectMode):     #ダイレクトモード 
            selected_df = self.SelectRowByID(ID)
            #IDがIndexとなる行が存在するか確認、また固有かどうか
            if(selected_df.empty):
                self.err = Error.NO_ROW_EXIST
                return False
            if(len(selected_df) != 1):
                self.err = Error.DATA_NOT_UNIQUE_BY_ID
                return False
            #行の更新           
            for key in UpdateDict:
                ValueType = self.Column_DF[self.Column_DF[self.col_inf_columns[3]] == key][self.col_inf_columns[5]]
                if(ValueType.empty):
                    self.err = Error.INVALID_COLUMN_NAME
                    return False
                if(type(UpdateDict[key]) != Access_dtype_py[ValueType.values[0]]):
                    self.err = Error.DATA_TYPE_MISMATCH
                    return False
                selected_df.at[ID,key] = UpdateDict[key]
            sql = self.__UpdateSQL(selected_df)
            self.cursor.execute(sql[0])
            self.conn.commit()
            ret_bool = True                    
            
        else:   #内部データフレームモード
            #IDがIndexとなる行が存在するか確認        
            if(self.Int_DF[self.Int_DF.index == ID].empty):
                self.err = Error.NO_ROW_EXIST
                return False
            #行の更新        
            for key in UpdateDict:             
                ValueType = self.Column_DF[self.Column_DF[self.col_inf_columns[3]] == key][self.col_inf_columns[5]]
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
                    self.RowState_DF.at[ID,'RowState'] = DataRowState.Updated
                elif(self.RowState_DF.at[ID,'RowState'] == DataRowState.Added):
                    self.Int_DF.at[ID,key] = UpdateDict[key]
                elif(self.RowState_DF.at[ID,'RowState'] == DataRowState.Deleted):
                    pass
            ret_bool =True
            
        return ret_bool
    
    def UpdateRowByDataFrame(self, df:pd.DataFrame) -> bool:
        """データベースにDataFaremeで行を更新する。（今のところDirectモードのみ）

        Args:
            df (pd.DataFrame): 更新する行のデータフレーム（1行のみ）

        Returns:
            bool: 成功=True / 失敗=False
        """
        ret_bool:bool = False
        df = df.replace([None],[float("nan")]).replace(["None"],[float("nan")]) #NoneをNaNに統一
        df = df.dropna(axis=1) #空白列削除
        if(self.DirectMode):    #ダイレクトモード
            if len(df) == 1:
                chk_id = df.index[0]
                chk_df = self.SelectRowByID(chk_id) #データベースのレコード確認
                if len(chk_df) > 0:                    
                    chk_df = chk_df.replace([None],[float("nan")]).replace(["None"],[float("nan")]) #NoneをNaNに統一
                    chk_df = chk_df.dropna(axis=1) #空白列削除
                    drop_columns = list(set(chk_df.columns) - set(df.columns)) #既存データ列のうち変更データにない列
                    chk_df = chk_df.drop(columns=drop_columns) #不要な列を落とす
                    add_columns = list(set(df.columns) - set(chk_df.columns)) #変更データ列にあり既存データ列にない列
                    if len(add_columns) > 0:
                        add_df = pd.DataFrame(columns=add_columns, index=df.index) #追加するDF
                        chk_df = pd.concat([chk_df,add_df],axis=1) #DF追加
                        chk_df = chk_df[df.columns] #列の順番をそろえる
                    update_df = df.T[df.T!=chk_df.T].T.dropna(axis=1).dropna(how="all") #変更するべきデータのみを抽出
                    if not(update_df.empty):                  
                        sql = self.__UpdateSQL(update_df)
                        self.cursor.execute(sql[0])
                        self.conn.commit()
                        ret_bool = True           
        return ret_bool
            
    def AddRow(self, AddDict:Dict[str,Any],ID:Union[int,str]=None) -> bool:
        """内部データフレームまたはデータベースに行を追加する。

        Args:
            AddDict (Dict[str,Any]): 追加する内容<列名,値>
            ID (Union[int,str], optional): ID、Noneで自動取得. Defaults to None.

        Returns:
            bool: 成功=True / 失敗=False
        
        Remarks:
            データフレームモード: 内部データフレームへ追加、データベースを更新（同期）させるまで変更されない。UpdateDataBase()
            ダイレクトモード: データベースが直接追加される。
        """
        ret_bool:bool        
        #データ型の確認
        column_names = self.Column_DF[self.col_inf_columns[3]]
        column_type_tag = self.col_inf_columns[5]
        for key in AddDict:
            ValueType = self.Column_DF[column_names == key][column_type_tag]
            if(ValueType.empty):
                self.err = Error.INVALID_COLUMN_NAME
                return False
            if(type(AddDict[key]) != Access_dtype_py[ValueType.values[0]]):
                self.err = Error.DATA_TYPE_MISMATCH
                return False
        
            
        if(self.DirectMode):    #ダイレクトモード
            if type(ID) == int or type(ID) == str:
                AddDict[column_names.iloc[0]] = ID
                new_row = pd.DataFrame([AddDict],columns=column_names)
                new_row = new_row.set_index(column_names.iloc[0])
            else:
                new_row = pd.DataFrame([AddDict], columns=column_names)
            
            sql = self.__InsertSQL(new_row)
            self.cursor.execute(sql[0])
            self.conn.commit()
            ret_bool = True
        else:   #内部データフレームモード
            #行の追加
            if(type(ID) == type(None)):
                new_id:Union[int,str] = self.Int_DF.index.max() + 1
            else:
                new_id = ID
            new_row = pd.DataFrame([AddDict],index=[new_id])
            self.Int_DF = pd.concat([self.Int_DF,new_row])
            self.RowState_DF.at[new_id,'RowState'] = DataRowState.Added
            ret_bool = True
        
        return ret_bool
    
    def AddRowByDataFrame(self, df:pd.DataFrame) -> bool:
        """データベースにDataFaremeで行を追加する。（今のところDirectモードのみ）

        Args:
            df (pd.DataFrame): 追加する行のデータフレーム

        Returns:
            bool: 成功=True / 失敗=False
        """
        ret_bool:bool
        if(self.DirectMode):    #ダイレクトモード            
            sql = self.__InsertSQL(df)
            self.cursor.execute(sql[0])
            self.conn.commit()
            ret_bool = True
        
        return ret_bool
    
    def DeleteRow(self, ID:Union[int,str], Del:bool=True) -> bool:
        """内部データフレームまたはデータベースの行を削除する(RowStateのみ変更)。

        Args:
            ID (int, str): 削除する行ID
            Del (bool, optional): 削除=True / 削除を解除=False、ダイレクトモードでは解除できない. Defaults to True.

        Returns:
            bool: 成功=True / 失敗=False
            
        Remarks:
            データフレームモード: 内部データフレームから削除、データベースを更新（同期）させるまで変更されないUpdateDataBase()。変更・追加した行は削除できない。一度データベースと同期をとった後削除してください。
            ダイレクトモード: データベースから直接削除される。            
        """
        ret_bool:bool                
        if(self.DirectMode):    #ダイレクトモード 
            del_df = self.SelectRowByID(ID)
            if(del_df.empty):
                self.err = Error.NO_ROW_EXIST
                return False
            sql = self.__DeleteSQL(del_df)
            self.cursor.execute(sql[0])
            self.conn.commit()
            ret_bool = True
            
        else:   #データフレームモード
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
            ret_bool = True
        
        return ret_bool
    
    def UpdateDataBase(self) -> bool:
        """データベースを内部DataFrameで更新する（同期）。ダイレクトモードでは動作しない。

        Returns:
            bool: 成功=True / 失敗=False
        """
        #ダイレクトモードでは動作しない
        if(self.DirectMode): 
            self.err = Error.NOT_WORK_THIS_MODE
            return False        
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

    def AddColumn_DataBase(self, ColmunName:str, DataType:AccessDataType, param_list:list=[]) -> bool:
        """データベースへ列を追加する。

        Args:
            ColmunName (str): 追加する行名
            DataType (AccessDataType): データ型
            param_list (list, optional): パラメータ. Defaults to [].

        Returns:
            bool: 成功=True / 失敗=False
        
        Remarks param_list:
            DataType = CHAR: [max length]
            DataType = VARCHAR: [max length]
            (Not Available) DataType = DECIMAL: [total digits, digits after the decimal point]
        
        """
        sql = f"ALTER TABLE {self.TableName} ADD COLUMN "            
        if(DataType == AccessDataType.CHAR):
            if(len(param_list) < 1):
                self.err = Error.INVALID_INPUT
                return False
            sql += f"{ColmunName} {DataType.name}({param_list[0]});"
        elif(DataType == AccessDataType.VARCHAR):
            if(len(param_list) < 1):
                self.err = Error.INVALID_INPUT
                return False
            sql += f"{ColmunName} {DataType.name}({param_list[0]});"
        elif(DataType == AccessDataType.MEMO):
            sql += f"{ColmunName} {DataType.name};"
        elif(DataType == AccessDataType.BYTE):
            sql += f"{ColmunName} {DataType.name};"
        elif(DataType == AccessDataType.INTEGER):
            sql += f"{ColmunName} {DataType.name};"
        elif(DataType == AccessDataType.LONG):
            sql += f"{ColmunName} {DataType.name};"
        elif(DataType == AccessDataType.SINGLE):
            sql += f"{ColmunName} {DataType.name};"
        elif(DataType == AccessDataType.DOUBLE):
            sql += f"{ColmunName} {DataType.name};"
        elif(DataType == AccessDataType.CURRENCY):
            sql += f"{ColmunName} {DataType.name};"
        elif(DataType == AccessDataType.DECIMAL):
            self.err = Error.INVALID_INPUT
            return False
            # if(len(param_list) < 2):
            #     self.err = Error.INVALID_INPUT
            #     return False
            # sql += f"{ColmunName} {DataType.name}({param_list[0]},{param_list[1]});"
        elif(DataType == AccessDataType.AUTOINCREMENT):
            sql += f"{ColmunName} {DataType.name};"
        elif(DataType == AccessDataType.DATE):
            sql += f"{ColmunName} {DataType.name};"
        elif(DataType == AccessDataType.TIME):
            sql += f"{ColmunName} {DataType.name};"
        elif(DataType == AccessDataType.DATETIME):
            sql += f"{ColmunName} {DataType.name};"
        elif(DataType == AccessDataType.TIMESTAMP):
            sql += f"{ColmunName} {DataType.name};"
        elif(DataType == AccessDataType.YESNO):
            sql += f"{ColmunName} {DataType.name};"
        elif(DataType == AccessDataType.OLEOBJECT):
            sql += f"{ColmunName} {DataType.name};"
        elif(DataType == AccessDataType.HYPERLINK):
            self.err = Error.INVALID_INPUT
            return False
            #sql += f"{ColmunName} {DataType.name};"
        elif(DataType == AccessDataType.GUID):
            sql += f"{ColmunName} {DataType.name};"
        else:
            self.err = Error.INVALID_INPUT
            return False
                
        self.cursor.execute(sql)
        self.conn.commit()
        self.__GetColumnNameFromDataBase()
        return True    
    
    def DeleteColumn_DataBase(self, ColumnName:str) -> bool:
        """データベースから列を削除する。

        Args:
            ColumnName (str): 削除する列名

        Returns:
            bool: 成功=True / 失敗=False
        """
        sql = f"ALTER TABLE {self.TableName} DROP COLUMN {ColumnName};"
        self.cursor.execute(sql)
        self.conn.commit()
        self.__GetColumnNameFromDataBase()
        return True
    
    def AddTable_DataBase(self, PriKeyInf:tuple[str,AccessDataType], param:Optional[int]=None) -> bool:
        """データベースにテーブルを追加する

        Args:
            PriKeyInf (tuple[str,AccessDataType]): プライマリKey情報tuple(列名,データ型)、通常列名は"ID"
            param (Optional[int], optional): 文字列の場合文字数. Defaults to None.

        Returns:
            bool: 成功=True / 失敗=False
        """
        ret_bool:bool = False
        sql:str
        if self.IsTableExist():
            ret_bool = False            
        else:
            if type(param) == type(None):
                sql = f"CREATE TABLE {self.TableName} ({PriKeyInf[0]} {PriKeyInf[1].name} PRIMARY KEY);"
            elif type(param) == int:
                sql = f"CREATE TABLE {self.TableName} ({PriKeyInf[0]} {PriKeyInf[1].name}({param}) PRIMARY KEY);"
            else:
                sql=""
            if len(sql) > 0:
                self.cursor.execute(sql)
                self.conn.commit()
                self.__GetColumnNameFromDataBase()
                ret_bool = True
        return ret_bool
    
    def __SelectSQL(self, Data:Dict[str,Any]=None,
                    Serch_condition:SerchCondition=SerchCondition.Exact
                    ) -> str:
        """SELECTのSQL

        Args:
            Data (Dict[str,Any], optional): 検索データ / Noneで全データ. Defaults to None.
            Serch_condition (SerchCondition, optional):検索条件. Defaults to SerchCondition.Exact.

        Returns:
            str: SQLコマンド文字列
            
        Remarks:
            Data:検索データが文字列で*を含む場合は曖昧検索になる
            
        Todo:
            OR検索の対応。現状はAND検索のみ対応
        """        
        col_name_tag = self.col_inf_columns[3]
        column_names = self.Column_DF[col_name_tag]
        col_type_tag = self.col_inf_columns[5]
        sql_str = f'SELECT * FROM [{self.TableName}]'
        if(type(Data) == type(None)):
            return sql_str
        elif(type(Data) != dict):
            self.err = Error.INVALID_INPUT
            return ''
               
        sql_str += ' WHERE'
        for i,key in enumerate(Data):             
            py_dtype = Access_dtype_py[self.Column_DF[column_names==key][col_type_tag][0]]
            if(not(key in column_names.to_list())):
                self.err = Error.INVALID_COLUMN_NAME
                return ''
            if i > 0:
                sql_str += " AND "                                
            if type(Data[key]) == int and py_dtype == int:
                if(Serch_condition == SerchCondition.Exact):
                    sql_str += f' {key} = {Data[key]}'
                elif(Serch_condition == SerchCondition.SmallerThan):
                    sql_str += f' {key} < {Data[key]}'
                elif(Serch_condition == SerchCondition.OrSmallerThan):
                    sql_str += f' {key} <= {Data[key]}'
                elif(Serch_condition == SerchCondition.LargerThan):
                    sql_str += f' {key} > {Data[key]}'
                elif(Serch_condition == SerchCondition.OrLargerThan):
                    sql_str += f' {key} >= {Data[key]}'
                else:
                    self.err = Error.SELECT_CONDITION_ERR
                    return ""
            elif type(Data[key]) == float and py_dtype == float:
                if(Serch_condition == SerchCondition.Exact):
                    sql_str += f' {key} = {Data[key]}'
                elif(Serch_condition == SerchCondition.SmallerThan):
                    sql_str += f' {key} < {Data[key]}'
                elif(Serch_condition == SerchCondition.OrSmallerThan):
                    sql_str += f' {key} <= {Data[key]}'
                elif(Serch_condition == SerchCondition.LargerThan):
                    sql_str += f' {key} > {Data[key]}'
                elif(Serch_condition == SerchCondition.OrLargerThan):
                    sql_str += f' {key} >= {Data[key]}'
                else:
                    self.err = Error.SELECT_CONDITION_ERR
                    return ""
            elif type(Data[key]) == Decimal and py_dtype == Decimal:
                if(Serch_condition == SerchCondition.Exact):
                    sql_str += f' {key} = {Data[key]}'
                elif(Serch_condition == SerchCondition.SmallerThan):
                    sql_str += f' {key} < {Data[key]}'
                elif(Serch_condition == SerchCondition.OrSmallerThan):
                    sql_str += f' {key} <= {Data[key]}'
                elif(Serch_condition == SerchCondition.LargerThan):
                    sql_str += f' {key} > {Data[key]}'
                elif(Serch_condition == SerchCondition.OrLargerThan):
                    sql_str += f' {key} >= {Data[key]}'
                else:
                    self.err = Error.SELECT_CONDITION_ERR
                    return ""
            elif type(Data[key]) == str and py_dtype == str:
                if(str(Data[key]).find('*')>=0):                        
                    sql_str += f' {key} LIKE \'{str(Data[key]).translate(wild_card)}\''
                else:
                    if(Serch_condition == SerchCondition.Exact):
                        sql_str += f' {key} = \'{Data[key]}\''
                    elif(Serch_condition == SerchCondition.StartWith):
                        sql_str += f" {key} LIKE \'{Data[key]}%\'"
                    elif(Serch_condition == SerchCondition.EndWith):
                        sql_str += f" {key} LIKE \'%{Data[key]}\'"
                    elif(Serch_condition == SerchCondition.Contains):
                        sql_str += f" {key} LIKE \'%{Data[key]}%\'"
                    else:
                        self.err = Error.SELECT_CONDITION_ERR
                        return ""
            elif type(Data[key]) == bool and py_dtype == bool:
                if(Data[key]):
                    sql_str += f' {key} = 1'
                else:
                    sql_str += f' {key} = 0'
            elif type(Data[key]) == datetime and py_dtype == datetime:
                sql_str += f' {key} = \'{datetime(Data[key]).strftime("%Y-%m-%d %H:%M:%S")}\''
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
        col_name_ser = self.Column_DF[self.col_inf_columns[3]]
        sql_Data = Data.replace([None],float("nan")).replace(["None"],float("nan"))
        sql_Data = sql_Data.dropna(axis=1)
        if(sql_Data.empty):
            self.err = Error.INVALID_INPUT
            return []              
        for row in sql_Data.iterrows():
            sql_str = f'UPDATE [{self.TableName}] SET'
            idx = row[0]
            content_ser = row[1]
            for col,val in content_ser.items():                
                AccCol_dtype = self.Column_DF[col_name_ser == col][self.col_inf_columns[5]].values[0]
                if(AccCol_dtype == AccessDataType.CHAR or
                   AccCol_dtype == AccessDataType.VARCHAR or
                   AccCol_dtype == AccessDataType.MEMO or
                   AccCol_dtype == AccessDataType.GUID):                        
                    sql_str += f' {col} = \'{val}\','
                elif(AccCol_dtype == AccessDataType.BYTE or
                     AccCol_dtype == AccessDataType.INTEGER or
                     AccCol_dtype == AccessDataType.LONG or
                     AccCol_dtype == AccessDataType.AUTOINCREMENT):
                    sql_str += f' {col} = {val},'
                elif(AccCol_dtype == AccessDataType.SINGLE or 
                     AccCol_dtype == AccessDataType.DOUBLE or
                     AccCol_dtype == AccessDataType.REAL):
                    sql_str += f' {col} = {val},'
                elif(AccCol_dtype == AccessDataType.CURRENCY):
                    sql_str += f' {col} = {val},'
                elif(AccCol_dtype == AccessDataType.DATE):
                    datetime_py:date = val
                    sql_str += f' {col} = \'{datetime_py.strftime("%Y/%m/%d")}\','
                elif(AccCol_dtype == AccessDataType.TIME):                    
                    datetime_py:time = val
                    sql_str += f' {col} = \'{datetime_py.strftime("%H:%M:%S")}\','
                elif(AccCol_dtype == AccessDataType.DATETIME):                    
                    datetime_py:datetime = val
                    sql_str += f' {col} = \'{datetime_py.strftime("%Y/%m/%d %H:%M:%S")}\','
                elif(AccCol_dtype == AccessDataType.TIMESTAMP):
                    sql_str += f' {col} = \'{val}\','                
                elif(AccCol_dtype == AccessDataType.YESNO):
                    if(val):
                        sql_str += f' {col[0]} = 1,'
                    else:
                        sql_str += f' {col[0]} = 0,'
                elif(AccCol_dtype == AccessDataType.OLEOBJECT):
                    sql_str += f' {col} = {val},'
                elif(AccCol_dtype == AccessDataType.VARBINARY):
                    datetime_py:time = val
                    sql_str += f' {col} = \'{datetime_py.strftime("%H:%M:%S.%f")}\','                                                  
                else:
                    self.err = Error.UNDEFINED_DATA_TYPE
                    return [""]
            if(type(idx) == int):
                sql_str = sql_str[0:-1] + f' WHERE {sql_Data.index.name} = {idx};'
            elif(type(idx) == str):
                sql_str = sql_str[0:-1] + f' WHERE {sql_Data.index.name} = \'{idx}\';'
            
            out_str_list.append(sql_str)
        return out_str_list
            
    def __InsertSQL(self, Data:pd.DataFrame) -> List[str]:
        """INSERTのSQL

        Args:
            Data (pd.DataFrame): Insertするデータ

        Returns:
            List[str]: SQLコマンド文字列リスト
        """        
        out_str_list:List[str] = []
        col_name_ser = self.Column_DF[self.col_inf_columns[3]]
        col_dtype_tag = self.col_inf_columns[5]
        sql_Data = Data.replace([None],float("nan")).replace(["None"],float("nan"))
        sql_Data = sql_Data.dropna(axis=1)      
        if(sql_Data.empty):
            self.err = Error.INVALID_INPUT
            return []
        for row in sql_Data.iterrows():
            idx = row[0]
            content_ser = row[1]
            sql_str = f'INSERT INTO [{self.TableName}]'
            sql_col = '('
            sql_val = 'VALUES ('
            if type(idx) == int:
                sql_col += f"{sql_Data.index.name},"
                sql_val += f"{idx}"
            elif type(idx) == str:
                sql_col += f"{sql_Data.index.name},"
                sql_val += f"\'{idx}\',"                            
            for col,val in content_ser.items():                
                AccCol_dtype = self.Column_DF[col_name_ser == col][col_dtype_tag].values[0]
                if(AccCol_dtype == AccessDataType.CHAR or
                   AccCol_dtype == AccessDataType.VARCHAR or
                   AccCol_dtype == AccessDataType.MEMO or
                   AccCol_dtype == AccessDataType.GUID):
                    sql_col += f'{col}, '
                    sql_val += f'\'{val}\', '
                elif(AccCol_dtype == AccessDataType.BYTE or
                     AccCol_dtype == AccessDataType.INTEGER or
                     AccCol_dtype == AccessDataType.LONG or
                     AccCol_dtype == AccessDataType.AUTOINCREMENT):
                    sql_col += f'{col}, '
                    sql_val += f'{val}, '                    
                elif(AccCol_dtype == AccessDataType.SINGLE or 
                     AccCol_dtype == AccessDataType.DOUBLE or
                     AccCol_dtype == AccessDataType.REAL):
                    sql_col += f'{col}, '
                    sql_val += f'{val}, '                    
                elif(AccCol_dtype == AccessDataType.CURRENCY):
                    sql_col += f'{col}, '
                    sql_val += f'{val}, '
                elif(AccCol_dtype == AccessDataType.DATE):
                    datetime_py:date = val
                    sql_col += f'{col}, '
                    sql_val += f' \'{datetime_py.strftime("%Y/%m/%d")}\', '
                elif(AccCol_dtype == AccessDataType.TIME):                    
                    datetime_py:time = val
                    sql_col += f'{col}, '
                    sql_val += f'\'{datetime_py.strftime("%H:%M:%S")}\', '
                elif(AccCol_dtype == AccessDataType.DATETIME):                    
                    datetime_py:datetime = val
                    sql_col += f'{col}, '
                    sql_val += f'\'{datetime_py.strftime("%Y/%m/%d %H:%M:%S")}\', '
                elif(AccCol_dtype == AccessDataType.TIMESTAMP):
                    sql_col += f'{col}, '
                    sql_val += f'\'{val}\', ' 
                elif(AccCol_dtype == AccessDataType.YESNO):                    
                    if(val):
                        sql_col += f'{col}, '
                        sql_val += '1, '
                    elif(not(val)):
                        sql_col += f'{col}, '
                        sql_val += '0, '
                elif(AccCol_dtype == AccessDataType.OLEOBJECT):
                    sql_col += f'{col}, '
                    sql_val += f'{val}, '
                elif(AccCol_dtype == AccessDataType.VARBINARY):
                    sql_col += f'{col}, '
                    sql_val += f'\'{datetime_py.strftime("%H:%M:%S.%f")}\', '
                  
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
        for row in Data.iterrows():
            idx = row[0]
            content_ser = row[1]
            sql_str = f'DELETE FROM [{self.TableName}] '
            sql_str += 'WHERE '
            sql_str += f'{Data.index.name} = '
            if(type(idx)==int):
                sql_str += f'{idx};'
            elif(type(idx)==str):
                sql_str += f'\'{idx}\';'
            out_str_list.append(sql_str)
        
        return out_str_list                       

    def __SqlResultToDataFrame(self, Res:List[pyodbc.Row], set_index:Optional[str]='ID') -> pd.DataFrame:
        """SQLの結果をデータフレームへ変換する

        Args:
            Res (List[pyodbc.Row]): SQLの結果
            set_index (Optional[str], optional): インデックスにする行名. Defaults to 'ID'.

        Returns:
            pd.DataFrame: 変換後のデータフレーム
        """
        out_df = pd.DataFrame()
        if(len(Res)<1):
            self.err = Error.NO_DATA_IN_TABLE
            return out_df        
        #データフレーム構築
        out_df = pd.DataFrame(np.array(Res,dtype=object), columns=[c for c in self.Column_DF[self.col_inf_columns[3]]])
        if(type(set_index) == str):
            out_df = out_df.set_index(set_index)
        return out_df
    
    def __GetColumnNameFromDataBase(self):
        """データベースの列情報を取得してクラス内のDataFrameをアップデートする。
        """        
        cols_inf = self.cursor.columns(table=self.TableName)
        cols_inf_res = cols_inf.fetchall()           
        self.Column_DF = pd.DataFrame(np.array(cols_inf_res),columns=self.col_inf_columns)
        for row in self.Column_DF.iterrows():
            for access_dtype in AccessDataType:
                if self.Column_DF.loc[row[0],self.col_inf_columns[5]] == access_dtype.name:
                    self.Column_DF.loc[row[0],self.col_inf_columns[5]] = access_dtype
        
    def IsTableExist(self) -> bool:
        """データテーブルが存在するかどうか確認する。

        Returns:
            bool: 存在する=True | 存在しない=False
        """
        ret_bool:bool
        try:
            self.cursor.columns(table=self.TableName)            
            res = self.cursor.fetchall()
            if len(res) < 1:
                ret_bool = False
            else:
                ret_bool = True
        except pyodbc.ProgrammingError:
            ret_bool = False
        return ret_bool
        