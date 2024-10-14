import pymysql.cursors
import pandas as pd
from pandas import DataFrame,Series,Timestamp,Timedelta
from pandas._libs.tslibs import timedeltas,timestamps
from datetime import datetime,date,timedelta
from enum import Enum
from typing import List,Dict,Any,Tuple,Union,Optional
import numpy as np

wild_card = str.maketrans({'*':'%'})
"""Wilde Card Translate"""

class StarageEngine(Enum):
    """ストレージエンジン: see https://dev.mysql.com/doc/refman/8.0/en/storage-engines.html"""
    InnoDB = 0
    """(DEFAULT) Supports transactions, row-level locking, and foreign keys"""

class DataType(Enum):
    """行のデータタイプ see: https://dev.mysql.com/doc/refman/8.0/en/data-types.html"""
    TINYINT = 0
    """整数 1byte"""
    SMALLINT = 1
    """整数 2bytes"""
    MEDIUMINT = 2
    """整数 3bytes"""
    INT = 3
    """整数 4bytes"""
    BIGINT = 4
    """整数 8bytes"""
    DECIMAL = 5
    """実数 値を(総桁数, 小数点以下桁数)で指定必要"""
    FLOAT = 6
    """実数単精度"""
    DOUBLE = 7
    """実数倍精度"""
    REAL = 8
    """FLOATと同じ"""
    BIT = 9
    """Bit 値が(ビット数)で指定 """
    BOOLEAN = 10
    """Boolean"""
    SERIAL = 11
    """Serial Numeric"""
    DATE = 12
    """日付 YYYY-MM-DD"""
    DATETIME = 11
    """日付時間 YYYY-MM-DD hh:mm:ss"""
    TIMESTAMP = 12
    """タイムスタンプ YYYY-MM-DD hh:mm:ss UTC"""
    TIME = 13
    """時間 hh:mm:ss"""
    YEAR = 14
    """年 YYYY"""
    CHAR = 15
    """固定長 文字列 値が(長さ)で指定"""
    VARCHAR = 16
    """可変長 文字列 値が(最大長さ)で指定"""
    TINYTEXT = 17
    """文章"""
    TEXT = 18
    """文章"""
    MEDIUMTEXT = 19
    """文章"""
    LONGTEXT = 20
    """文章"""
    BINARY = 21
    """固定長バイナリ 値が(bytes)で指定"""
    VARBINARY = 22
    """可変長バイナリ 値が(最大bytes)で指定"""
    TINYBLOB = 23
    """TINYTEXTと同じ"""
    BLOB = 24
    """TEXTと同じ"""
    MEDIUMBLOB = 25
    """MEDIUMTETXと同じ"""
    LONGBLOB = 26
    """LONGTETXと同じ"""
    ENUM = 27
    """Enumeration 値が(値1、値2...)で指定 1 or 2 Bytes値"""
    SET = 28
    """Enumeration同様 値が(値1、値2...)で指定 8 Bytes値 max 64members"""
    GEOMETRY = 29
    """幾何学値"""
    POINT = 30
    """点 (x,y) がデータフォーマット """
    LINESTRING = 31
    """ライン文字列 (x1 y1, x2 y2, x3 y3, x4 y4) 4点がデータフォーマット """
    POLYGON = 32
    """A Polygon with one exterior ring and one interior ring: POLYGON((0 0,10 0,10 10,0 10,0 0),(5 5,7 5,7 7,5 7, 5 5)) """
    MULTIPOINT = 33
    """A MultiPoint with three Point values: MULTIPOINT(0 0, 20 20, 60 60)"""
    MULTILINESTRING = 34
    """A MultiLineString with two LineString values: MULTILINESTRING((10 10, 20 20), (15 15, 30 15))"""
    MULTIPOLYGON = 35
    """A MultiPolygon with two Polygon values: MULTIPOLYGON(((0 0,10 0,10 10,0 10,0 0)),((5 5,7 5,7 7,5 7, 5 5)))"""
    GEOMETRYCOLLECTION = 36
    """A GeometryCollection consisting of two Point values and one LineString: GEOMETRYCOLLECTION(POINT(10 10), POINT(30 30), LINESTRING(15 15, 20 20))"""
    JSON = 37
    """JSON see:https://dev.mysql.com/doc/refman/8.0/en/json.html"""

class DefaultValue(Enum):
    """デフォルト値"""
    NoDafault = 0
    """なし"""
    UserDefined = 1
    """ユーザー定義 行指定 DEFAULT(col_name)"""
    NULL = 2
    """NULL"""
    CURRENT_TIME_STAMP = 3
    """今の時間Stamp """

class Attribute(Enum):
    """属性"""
    NONE = 0
    """指定なし"""
    BINARY = 1
    """バイナリ"""
    UNSIGNED = 2
    """符号なし、正の数"""
    UNSIGNED_ZEROFILL = 3
    """符号なし、0埋め"""
    on_update_CURRENT_TIMESTAMP = 4
    """DataType=CURRENT_TIMESTAMPで使用 更新時、タイムスタンプ更新"""
    COMPRESSED_zlib = 5
    """不明"""

class Index(Enum):
    """【未実装】インデクス"""
    NONE = 0
    """インデクスなし"""
    PRIMARY = 1
    """プライマリ"""
    UNIQUE = 2
    """固有"""
    INDEX = 3
    """ユーザー指定インデクス"""
    FULLTEXT = 4
    """FULLTEXT"""
    SPATIAL = 5
    """SPATIAL"""

class DataBaseCtrl():
    """MySQLデータベース制御クラス（クラス）
    """
    PriIdxName:str="ID"
    """第1列名、プライマリインデクスになる"""
    DataBaseName:str
    """データベース名"""
    err:Optional[pymysql.Error]
    """SQLエラー内容"""
    def __init__(self,DataBaseIP:str,DataBaseName:str,UserName:str,PassWord:str,CharSet:str="utf8mb4") -> None:
        """MySQLデータベース制御クラス（コンストラクター）

        Args:
            DataBaseIP (str): データベースIPアドレス
            DataBaseName (str): データベース名
            UserName (str): ユーザー名
            PassWord (str): パスワード
            CharSet (str, optional): 文字セット. Defaults to "utf8mb4".
        """
        try:
            self.connection = pymysql.connect(
                host = DataBaseIP,
                user = UserName,
                password = PassWord,
                db = DataBaseName,
                charset = CharSet,
                cursorclass=pymysql.cursors.DictCursor
            )
            self.DataBaseName = str(self.connection.db).split("'")[1]
            self.cursor = self.connection.cursor()
            self.err = None
        except pymysql.Error as err:
            self.err = err
        
    def __del__(self) -> None:
        if (self.cursor.connection != None):
            self.cursor.close()
        if (self.connection.open):
            self.connection.close()         

    def AddTable(
        self,
        TableName:str,
        ID_Type:DataType=DataType.INT,
        LEN_VAL:Optional[Union[str,int]]=None,
        Default_Value:DefaultValue=DefaultValue.NoDafault,
        Default_Value_User_define:Optional[str]=None,
        CharacterSet:Optional[str]=None,
        AttributeSet:Attribute=Attribute.NONE,
        Nullable:bool=True,
        AutoIncriment:bool=False,
        Engine:StarageEngine=StarageEngine.InnoDB,
        Comment:Optional[str]=None
        ) -> bool:
        """_summary_

        Args:
            TableName (str): テーブル名
            ID_Type (DataType, optional): 主Key=IDのデータタイプ. Defaults to DataType.INT.
            LEN_VAL (Optional[Union[str,int]], optional): 長さ/値（Typeによって要否、フォーマット異なる）. Defaults to None.
            Default_Value (DefaultValue, optional): デフォルト値. Defaults to DefaultValue.NoDafault.
            Default_Value_User_define (Optional[str], optional): デフォルト値がユーザー定義の場合の値. Defaults to None.
            CharacterSet (Optional[str], optional): 【未実装】照合順序. Defaults to None.
            AttributeSet (Attribute, optional): 属性. Defaults to Attribute.NONE.
            Nullable (bool, optional): NULL値を許可. Defaults to True.
            AutoIncriment (bool, optional): オートインクリメント. Defaults to False.
            Engine (StarageEngine, optional): ストレージエンジン（Defaultのみ実装）. Defaults to StarageEngine.InnoDB.
            Comment (Optional[str], optional): テーブルコメント. Defaults to None.

        Returns:
            bool: True=成功/False=エラー
        """
        sql = f"CREATE TABLE {self.DataBaseName}.{TableName} "
        result,column_sql,index_sql =self.__MakeColumnSql(
            ColumnName="ID",
            Type=ID_Type,
            LEN_VAL=LEN_VAL,
            Default_Value=Default_Value,
            Default_Value_User_define=Default_Value_User_define,
            CharacterSet=CharacterSet,
            AttributeSet=AttributeSet,
            Nullable=Nullable,
            IndexSet=Index.PRIMARY,
            IndexValue=None,
            AutoIncriment=AutoIncriment,
            Comment="IDは主Keyです。"
        )
        if result:
            sql += f"({column_sql},{index_sql}) "
            sql += f"ENGINE = {Engine.name}  COMMENT = '{Comment}';"
            try:
                self.cursor.execute(sql)
                self.cursor.fetchall()
                self.err = None
                result = True
            except pymysql.Error as err:
                self.err = err
                result = False
        return result
    
    def DeleteTable(self,TableName:str) -> bool:
        """テーブルを削除する。

        Args:
            TableName (str): テーブル名

        Returns:
            bool: True=成功/False=エラー
        """
        result = True
        sql = f"DROP TABLE {TableName};"
        try:
            self.cursor.execute(sql)
            self.cursor.fetchall()
            result = True
        except pymysql.Error as err:
            self.err = err
            result = False
        return result
       
    def IsExistTable(self,TableName:str) -> Tuple[bool,bool]:
        """テーブルが存在するかどうか確認する。

        Args:
            TableName (str): テーブル名

        Returns:
            Tuple[bool,bool]: SQLエラー, テーブル存在(True)
        """
        sql = "SELECT COUNT(*) FROM information_schema.tables "
        sql += f"WHERE table_schema = '{self.DataBaseName}' "
        sql += f"AND table_name = '{TableName}';"
        try:
            res = self.cursor.execute(sql)
            res = self.cursor.fetchall()
            self.err = None
            out_val = res[0]["COUNT(*)"] == 1
            err_bool = True
        except pymysql.Error as err:
            self.err = err
            out_val = False
            err_bool = False
        return err_bool,out_val
       
    def AddColumn(
        self,
        TableName:str,
        ColumnName:str,
        Type:DataType,
        LEN_VAL:Optional[Union[str,int]]=None,
        Default_Value:DefaultValue=DefaultValue.NoDafault,
        Default_Value_User_define:Optional[Union[str,int]]=None,
        CharacterSet:Optional[str]=None,
        AttributeSet:Attribute=Attribute.NONE,
        Nullable:bool=True,
        IndexSet:Index=Index.NONE,
        IndexValue:Optional[str]=None,
        AutoIncriment:bool=False,
        Comment:Optional[str]=None
        ) -> bool:
        """データベースに列を追加する。

        Args:
            TableName (str): テーブル名
            ColumnName (str): 追加する列名
            Type (DataType): データタイプ
            LEN_VAL (str | int, optional): 長さ/値（Typeによって要否、フォーマット異なる）. Defaults to None.
            Default_Value (DefaultValue, optional): デフォルト値. Defaults to Default_Value.NoDafault.
            Default_Value_User_define (str | int , optional): デフォルト値がユーザー定義の場合の値. Defaults to None.
            CharacterSet (Optional[str], optional): 【未実装】照合順序. Defaults to None.
            AttributeSet (Attribute, optional): 属性. Defaults to Attribute.NONE.
            Nullable (bool, optional): NULL値を許可. Defaults to True.
            IndexSet (Index, optional): インデクス設定. Defaults to Index.NONE.
            IndexValue (str, optional): インデクス設定値. Defaults to None.
            AutoIncriment (bool, optional): オートインクリメント. Defaults to False.
            Comment (Optional[str], optional): コメント. Defaults to None.

        Returns:
            bool: True=成功/False=エラー
        """        
        sql = "ALTER TABLE "
        sql += f"{TableName} ADD "
        result = True
        
        ##### 列設定の作成・追加 ##### 
        result,column_sql,Index_sql = self.__MakeColumnSql(
            ColumnName=ColumnName,
            Type=Type,
            LEN_VAL=LEN_VAL,
            Default_Value=Default_Value,
            Default_Value_User_define=Default_Value_User_define,
            CharacterSet=CharacterSet,
            AttributeSet=AttributeSet,
            Nullable=Nullable,
            IndexSet=IndexSet,
            IndexValue=IndexValue,
            AutoIncriment=AutoIncriment,
            Comment=Comment)
        
        if result:
            sql += column_sql
        else:
            return result
        
        ##### 最後の列に追加 #####
        if result:
            last_column = self.__GetLastColumnName(TableName)
            sql += f"AFTER {last_column}"
            
        ##### インデックス設定の追加 #####
        if Index_sql == None:
            sql += ";"
        else:
            sql += f", ADD {Index_sql};"            
            
        ##### SQLの実行 #####
        if result:
            try:
                self.cursor.execute(sql)
                self.cursor.fetchall()
                self.err = None
            except pymysql.Error as err:
                self.err = err
                result = False
                    
        return result
    
    def DeleteColumn(self,TableName:str,ColumunName:str) -> bool:        
        """テーブルから行を削除する。

        Args:
            TableName (str): テーブル名
            ColumunName (str): 行名

        Returns:
            bool: True=成功/False=エラー
        """
        sql = f"ALTER TABLE {TableName} DROP {ColumunName};"
        try:
            self.cursor.execute(sql)
            self.cursor.fetchall()
            self.err = None
            result = True
        except pymysql.Error as err:
            self.err = err
            result = False
        return result

    def OptimizeTable(self,TableName:str) -> bool:
        """テーブルを最適化する。

        Args:
            TableName (str): テーブル名

        Returns:
            bool: True=成功/False=エラー
        """
        sql = f"OPTIMIZE TABLE {TableName};"
        try:
            self.cursor.execute(sql)
            self.cursor.fetchall()
            self.err = None
            result = True
        except pymysql.Error as err:
            self.err = err
            result = False
        return result
            
    def GetColmunsInfo(self,TableName:str) -> List[Dict[str,Optional[str]]]:
        """行情報取得

        Args:
            TableName (str): テーブル名

        Returns:
            List[Dict[str,Optional[str]]]: 行情報
        """
        sql = f"SHOW COLUMNS FROM {TableName}"
        try:
            self.cursor.execute(sql)
            columuns = self.cursor.fetchall()
            self.err = None
        except pymysql.Error as err:
            self.err = err
        return columuns

    def GetRecordCount(self,TableName:str,ID:Optional[Union[int,str]]=None) -> Optional[int]:
        """IDが一致するレコード数、または全てのレコード数を取得する。

        Args:
            TableName (str): テーブル名
            ID (Optional[Union[int,str]], optional): ID（省略で全てのレコード数）. Defaults to None.

        Returns:
            Optional[int]: 条件に一致するレコード数。Noneはエラー
        """
        sql = f"SELECT COUNT(1) FROM {TableName}"
        if ID == None:
            sql += ";"
        elif type(ID) == str:
            sql += f" WHERE ID = '{ID}';"
        else:
            sql += f" WHERE ID = {ID};"
        try:
            self.cursor.execute(sql)
            res = self.cursor.fetchall()
            self.err = None
            out_val = int(res[0]["COUNT(1)"])
        except pymysql.Error as err:
            self.err = err
            out_val = None
        return out_val

    def GetRowByID(self,TableName:str,ID:Optional[Union[int,str]]=None) -> Optional[DataFrame]:
        """IDで一致する行、またはテーブルをDataFrameとして取得する。

        Args:
            TableName (str): テーブル名
            ID (Optional[Union[int,str]], optional): ID、省略で全データ. Defaults to None.

        Returns:
            Optional[DataFrame]: 条件に一致するDataFrame。エラーまたは該当データがない場合Noneを返す。
        """
        sql = f"SELECT * FROM {TableName}"
        if ID == None:
            pass
        else:
            sql += f" WHERE ID = {ID}"
        sql += ";"
        try:
            self.cursor.execute(sql)
            res = self.cursor.fetchall()
            self.err = None            
        except pymysql.Error as err:
            self.err = err
            return None
        if len(res) > 0:
            df = DataFrame(res)
            df.set_index("ID",inplace=True)
            df.Name = TableName
            df_format = self.GetDataFrameFormat(TableName)
            df.index = df.index.astype(df_format.index.dtype)
            for col in df_format.columns.to_list():
                if df[col].notna().all():
                    df[col] = df[col].astype(df_format[col].dtype)
        else:
            df = None
        return df
        
    def GetDataFrameFormat(self,TableName:str) -> DataFrame:
        """_summary_

        Args:
            TableName (str): テーブル名

        Returns:
            DataFrame: 行情報（名前、データタイプ）だけの空のDataFrame
        """
        column_info_list = self.GetColmunsInfo(TableName)
        df_dtypes = {}
        for column_info in column_info_list:
            type_val = column_info["Type"]            
            if type_val.count("tinyint") > 0:
                if type_val.count("unsigned") > 0:
                    df_dtypes[column_info["Field"]] = "uint8"
                else:
                    df_dtypes[column_info["Field"]] = "int8"
            elif type_val.count("smallint") > 0:
                if type_val.count("unsigned") > 0:
                    df_dtypes[column_info["Field"]] = "uint16"
                else:
                    df_dtypes[column_info["Field"]] = "int16"
            elif type_val.count("mediumint") > 0:
                if type_val.count("unsigned") > 0:
                    df_dtypes[column_info["Field"]] = "uint24"
                else:
                    df_dtypes[column_info["Field"]] = "int24"
            elif type_val == "bigint":
                if type_val.count("unsigned") > 0:
                    df_dtypes[column_info["Field"]] = "uint64"
                else:
                    df_dtypes[column_info["Field"]] = "int64"
            elif type_val.count("int"):
                if type_val.count("unsigned") > 0:
                    df_dtypes[column_info["Field"]] = "uint32"
                else:
                    df_dtypes[column_info["Field"]] = "int32"
            elif type_val.count("decimal") > 0:
                df_dtypes[column_info["Field"]] = "float64"
            elif type_val.count("float") > 0:
                df_dtypes[column_info["Field"]] = "float32"
            elif type_val.count("double") > 0:
                df_dtypes[column_info["Field"]] = "float64"
            elif type_val.count("real") > 0:
                df_dtypes[column_info["Field"]] = "float32"
            elif type_val.count("bit") > 0:
                df_dtypes[column_info["Field"]] = "int64"
            elif type_val.count("tinyint(1)") > 0:
                df_dtypes[column_info["Field"]] = "bool"
            elif type_val.count("serial") > 0:
                df_dtypes[column_info["Field"]] = "uint64"
            elif type_val.count("date") > 0:
                df_dtypes[column_info["Field"]] = "datetime64[ns]"
            elif type_val.count("datetime") > 0:
                df_dtypes[column_info["Field"]] = "datetime64[ns]"
            elif (type_val.count("year") > 0):
                df_dtypes[column_info["Field"]] = "uint16"
            elif type_val.count("timestamp") > 0:
                df_dtypes[column_info["Field"]] = "datetime64[ns]"
            elif type_val.count("time") > 0:
                df_dtypes[column_info["Field"]] = "timedelta64[ns]"
            elif (type_val.count("char") > 0 or
                  type_val.count("varchar") > 0 or
                  type_val.count("text") > 0):
                df_dtypes[column_info["Field"]] = str
            else:
                df_dtypes[column_info["Field"]] = "object"
         
        df = DataFrame({col:Series(dtype=dt) for col, dt in df_dtypes.items()})   
        df.Name = TableName
        df.set_index("ID",inplace=True)      
        return df.copy()

    def UpdateTable(self,TableName:str,Data:DataFrame,OverWrite:bool=False) -> Tuple[int,List[pymysql.Error]]:
        """DataFrameでデータベースに行を挿入または更新する。

        Args:
            TableName (str): テーブル名
            Data (DataFrame): データ、書き込むテーブルと同じ形式が必要
            OverWrite (bool, optional): IDが重複の場合上書き更新？(True:上書き). Defaults to False.

        Returns:
            Tuple[int,List[pymysql.Error]]: 挿入または更新された行数, エラーリスト
        """
        sql_list:List[str] = []
        table_info_list = self.GetColmunsInfo(TableName)
        for idx,row in Data.iterrows():
            if self.GetRecordCount(TableName,idx) > 0:
                if OverWrite:
                    sql = f"UPDATE {TableName}"
                    exist_row = self.GetRowByID(TableName,idx)
                    update_data = ""
                    for col,item in row.items():
                        if pd.notna(item) and item != "":
                            if item != exist_row.loc[idx,col]:
                                item = self.__ConvertToValuStr(item)
                                update_data += f"{col} = {item},"
                    if len(update_data) > 0:
                        update_data = update_data[0:-1]
                        if type(idx) in [int,float]:
                            sql += " SET " + update_data + f" WHERE ID = {idx};"
                        else:
                            sql += " SET " + update_data + f" WHERE ID = '{idx}';"
                            
                        sql_list.append(sql)
            else:
                sql = f"INSERT INTO {TableName}"
                cols_str = "(ID,"
                if type(idx) == str:
                     vals_str = f"('{idx}',"
                else:
                    vals_str = f"({idx},"
                for col,item in row.items():
                    if pd.isnull(item):
                        for table_info in table_info_list:
                            if table_info["Field"] == col:
                                if table_info["Null"] == "NO":
                                    cols_str += f"{col},"
                                    if (table_info["Type"].count("int") > 0 or
                                        table_info["Type"].count("float") > 0 or
                                        table_info["Type"].count("double") > 0) :
                                        vals_str += f"0,"
                                    elif table_info["Type"] == "date":
                                        vals_str += f"'0000-00-00',"
                                    elif (table_info["Type"] == "datetime" or
                                        table_info["Type"] == "timestamp"):
                                        vals_str += f"'0000-00-00 00:00:00',"
                                    elif table_info["Type"] == "time":
                                        vals_str += f"'00:00:00',"
                                    elif table_info["Type"].count("year") > 0:
                                        vals_str += f"0000,"
                                    else:
                                        vals_str += f"'',"
                                else:
                                    pass
                    elif item == "":
                        pass
                    else:
                        item = self.__ConvertToValuStr(item)
                        cols_str += f"{col},"
                        vals_str += f"{item},"
                cols_str = cols_str[0:-1] + ")"
                vals_str = vals_str[0:-1] + ")"
                sql += f" {cols_str} VALUES {vals_str};"
                sql_list.append(sql)
        
        update_count = 0
        err_list = []        
        for wsql in sql_list:
            try:
                self.cursor.execute(wsql)
                self.cursor.fetchall()
                update_count += 1
            except pymysql.Error as err:
                err_list.append(err)
        if update_count > 0:
            try:
                self.connection.commit()
            except pymysql.Error as err:
                err_list.append(err)
        
        return update_count,err_list

    def DeleteRows(self,TableName:str,ID_List:List[Union[int,str]]) -> Tuple[int,List[pymysql.Error]]:
        """データベースから行を削除する。

        Args:
            TableName (str): テーブル名
            ID_List (List[Union[int,str]]): 削除するIDリスト

        Returns:
            Tuple[int,List[pymysql.Error]]: 削除された行数, エラーリスト
        """
        sql_list:List[str] = []
        for id in ID_List:
            sql = f"DELETE FROM {TableName} WHERE ID = {id};"
            sql_list.append(sql)
        
        delete_count = 0
        err_list:List[pymysql.Error] = []    
        for sql in sql_list:
            try:
                self.cursor.execute(sql)
                self.cursor.fetchall()
                delete_count += 1
            except pymysql.Error as err:
                err_list.append(err)
        
        if delete_count > 0:
            self.connection.commit()
        
        return delete_count,err_list
        
    def __GetLastColumnName(self,TableName:str) -> str:
        """最後の行名を取得する。

        Args:
            TableName (str): テーブル名

        Returns:
            str: 最後の行名
        """
        columuns_info = self.GetColmunsInfo(TableName)
        return columuns_info[-1]["Field"]

    def __MakeColumnSql(
        self,        
        ColumnName:str,
        Type:DataType,
        LEN_VAL:Optional[Union[str,int]]=None,
        Default_Value:DefaultValue=DefaultValue.NoDafault,
        Default_Value_User_define:Optional[Union[str,int]]=None,        
        CharacterSet:Optional[str]=None,
        AttributeSet:Attribute=Attribute.NONE,
        Nullable:bool=True,
        IndexSet:Index=Index.NONE,
        IndexValue:Optional[str]=None,
        AutoIncriment:bool=False,
        Comment:Optional[str]=None        
        ) -> Tuple[bool,str,Optional[str]]:
        """列追加のSQLを作成する。

        Args:
            ColumnName (str): 追加する列名
            Type (DataType): データタイプ
            LEN_VAL (Optional[Union[str,int]], optional): 長さ/値（Typeによって要否、フォーマット異なる）. Defaults to None.
            Default_Value (DefaultValue, optional): デフォルト値. Defaults to DefaultValue.NoDafault.
            Default_Value_User_define (Optional[str | int], optional): デフォルト値がユーザー定義の場合の値. Defaults to None.
            CharacterSet (Optional[str], optional): 【未実装】照合順序. Defaults to None.
            AttributeSet (Attribute, optional): 属性. Defaults to Attribute.NONE.
            Nullable (bool, optional): NULL値を許可. Defaults to True.
            IndexSet (Index, optional): インデクス設定. Defaults to Index.NONE.
            IndexValue (Optional[str], optional): インデクス設定値. Defaults to None.
            AutoIncriment (bool, optional): オートインクリメント. Defaults to False.
            Comment (Optional[str], optional): コメント. Defaults to None.
        
        Returns:
            Tuple[bool,str,Optional[str]]: True:成功/False:エラー, 列Sql文字列, インデクスSql文字列
        """
        result = True
        sql = f"{ColumnName} " 
        ##### Data Type #####
        if Type == DataType.TINYINT:
            sql += "TINYINT"            
        elif Type == DataType.SMALLINT:
            sql += "SMALLINT"            
        elif Type == DataType.MEDIUMINT:
            sql += "MEDIUMINT"            
        elif Type == DataType.INT:
            sql += "INT"            
        elif Type == DataType.BIGINT:
            sql += "BIGINT"            
        elif Type == DataType.DECIMAL and LEN_VAL != None:
            if type(LEN_VAL) == str:
                if len(str(LEN_VAL).split(",")) == 2:
                    sql += f"DECIMAL({str(LEN_VAL)})"
                else:
                    result = False
            else:
                result = False                    
        elif Type == DataType.FLOAT:
            sql += "FLOAT"            
        elif Type == DataType.DOUBLE:
            sql += "DOUBLE"            
        elif Type == DataType.REAL:
            sql += "REAL"            
        elif Type == DataType.BIT and LEN_VAL != None:
            if type(LEN_VAL) == int:
                sql += f"BIT({str(LEN_VAL)})"
            else:
                result = False                
        elif Type == DataType.BOOLEAN:
            sql += "BOOLEAN"            
        elif Type == DataType.SERIAL:
            sql += "SERIAL"            
        elif Type == DataType.DATE:
            sql += "DATE"            
        elif Type == DataType.DATETIME:
            sql += "DATETIME"
            if LEN_VAL != None:
                sql += f"({str(LEN_VAL)})"            
        elif Type == DataType.TIMESTAMP:
            sql += "TIMESTAMP"
            if LEN_VAL != None:
                sql += f"({str(LEN_VAL)})"            
        elif Type == DataType.TIME:
            sql += "TIME"
            if LEN_VAL != None:
                sql += f"({str(LEN_VAL)})"
        elif Type == DataType.YEAR:
            sql += "YEAR"
        elif Type == DataType.CHAR and LEN_VAL != None:
            if type(LEN_VAL) in [int,np.int8,np.int16,np.int32,np.int64]:
                sql += f"CHAR({str(LEN_VAL)})"
            else:
                result = False
        elif Type == DataType.VARCHAR and LEN_VAL != None:
            if type(LEN_VAL) == int:
                sql += f"VARCHAR({str(LEN_VAL)})"
            else:
                result = False
        elif Type == DataType.TINYTEXT:
            sql += "TINYTEXT"
        elif Type == DataType.TEXT:
            sql += "TEXT"
        elif Type == DataType.MEDIUMTEXT:
            sql += "MEDIUMTEXT"
        elif Type == DataType.LONGTEXT:
            sql += "LONGTEXT"
        elif Type == DataType.BINARY and LEN_VAL != None:
            if type(LEN_VAL) == int:
                sql += f"BINARY({str(LEN_VAL)})"
            else:
                result = False
        elif Type == DataType.VARBINARY and LEN_VAL != None:
            if type(LEN_VAL) == int:
                sql += f"VARBINARY({str(LEN_VAL)})"
            else:
                result = False
        elif Type == DataType.TINYBLOB:
            sql += "TINYBLOB"
        elif Type == DataType.BLOB:
            sql += "BLOB"
        elif Type == DataType.MEDIUMBLOB:
            sql += "MEDIUMBLOB"
        elif Type == DataType.LONGBLOB:
            sql += "LONGBLOB"
        elif Type == DataType.ENUM and LEN_VAL != None:
            if type(LEN_VAL) == str:
                sql += f"ENUM({str(LEN_VAL)})"
            else:
                result = False
        elif Type == DataType.SET and LEN_VAL != None:
            if type(LEN_VAL) == str:
                sql += f"SET({str(LEN_VAL)})"
            else:
                result = False
        elif Type == DataType.GEOMETRY:
            sql += "GEOMETRY"
        elif Type == DataType.POINT:
            sql += "POINT"
        elif Type == DataType.LINESTRING:
            sql += "LINESTRING"
        elif Type == DataType.POLYGON:
            sql += "POLYGON"
        elif Type == DataType.MULTIPOINT:
            sql += "MULTIPOINT"
        elif Type == DataType.MULTILINESTRING:
            sql += "MULTILINESTRING"
        elif Type == DataType.MULTIPOLYGON:
            sql += "MULTIPOLYGON"
        elif Type == DataType.GEOMETRYCOLLECTION:
            sql += "GEOMETRYCOLLECTION"
        elif Type == DataType.JSON:
            sql += "JSON"
        else:
            result = False
            
        if result:
            sql += " "
        else:
            return result,None,None
        
        ##### 属性 #####
        if AttributeSet == Attribute.NONE:
            pass
        elif AttributeSet == Attribute.BINARY:
            sql += "BINARY "
        elif AttributeSet == Attribute.UNSIGNED:
            sql += "UNSIGNED "
        elif AttributeSet == Attribute.UNSIGNED_ZEROFILL:
            sql += "UNSIGNED ZEROFILL "
        elif AttributeSet == Attribute.on_update_CURRENT_TIMESTAMP and Type == DataType.TIMESTAMP:
            sql += "on update CURRENT_TIMESTAMP "
        else:
            result = False
            return result,None,None                
        
        ##### NULLABLE ##### 
        if Nullable:
            sql += "NULL "
        else:
            sql += "NOT NULL "
        
        ##### DEFAULT VALUE #####
        if Default_Value == DefaultValue.NoDafault:
            pass
        elif Default_Value == DefaultValue.UserDefined:
            if Default_Value_User_define != None:
                if type(Default_Value_User_define) == str:
                    sql += f"DEFAULT \'{str(Default_Value_User_define)}\' "
                elif type(Default_Value_User_define) == int:
                    sql += f"DEFAULT {str(Default_Value_User_define)} "
            else:
                result = False
        elif Default_Value == DefaultValue.NULL:
            sql += "DEFAULT NULL "
        elif Default_Value == DefaultValue.CURRENT_TIME_STAMP:
            sql += "DEFAULT CURRENT_TIMESTAMP "
        else:
            result = False
            return result,None,None
        
        ##### Auto Increment #####
        if AutoIncriment:
            sql += "AUTO_INCREMENT "
        else:
            pass
        
        ##### Comment #####
        if Comment != None:
            sql += f"COMMENT \'{Comment}\' "
        
        ##### インデックス #####
        idx_sql:Optional[str]=None
        if IndexSet==Index.NONE:
            pass
        elif IndexSet==Index.PRIMARY:
            idx_sql = f"PRIMARY KEY ({ColumnName})"
        elif IndexSet==Index.UNIQUE:
            if IndexValue==None:
                idx_sql = f"UNIQUE ({ColumnName})"
            else:
                idx_sql = f"UNIQUE {IndexValue} ({ColumnName})"
        elif IndexSet==Index.INDEX:
            if IndexValue==None:
                idx_sql = f"INDEX ({ColumnName})"
            else:
                idx_sql = f"INDEX {IndexValue} ({ColumnName})"
        elif IndexSet==Index.FULLTEXT:
            if IndexValue==None:
                idx_sql = f"FULLTEXT ({ColumnName})"
            else:
                idx_sql = f"FULLTEXT {IndexValue} ({ColumnName})"
        elif IndexSet==Index.SPATIAL:
            if IndexValue==None:
                idx_sql = f"SPATIAL ({ColumnName})"
            else:
                idx_sql = f"SPATIAL {IndexValue} ({ColumnName})"
        else:
            idx_sql = None
        
        return result,sql,idx_sql        

    def __ConvertToValuStr(self,PandasVal:Any) -> str:
        """PandasのフォーマットをSQLで書き込む文字列に変換する。

        Args:
            PandasVal (Any): Pandasのデータ

        Returns:
            str: SQL用文字列
        """
        if isinstance(PandasVal,(int,float)):
            out_val = f"{PandasVal}"
        elif isinstance(PandasVal,Timedelta):
            split_list = str(PandasVal).split(" ")
            out_val = f"'{split_list[-1]}'"
        else:
            out_val = f"'{PandasVal}'"
        return out_val
    
def AddRowToDataFrame(df:DataFrame,RowData:Dict[str,Any]) -> DataFrame:
    """DataFrameに行を追加する。

    Args:
        df (DataFrame): 元のDataFrame
        RowData (Dict[str,Any]): 追加する行データ

    Returns:
        DataFrame: 行追加後のDataFrame（エラーがある場合は元のDataFrameのまま出力する）
    """
    correct_dict:Dict[str,Any] = {}
    df_idx:Optional[Any] = None
    for key,val in RowData.items():
        if key in df.columns.to_list():
            correct_dict[key] = val
        if key=="ID":
            df_idx = val
    if df_idx != None and len(correct_dict) > 0:
        tmp_df1 = DataFrame(correct_dict,index=[df_idx])
        tmp_df1.index.name = "ID"
        tmp_df1.dropna(axis=1,inplace=True)
        tmp_df2 = tmp_df1.copy()
        for col in tmp_df1.columns.to_list():
            tmp_df2[col] = tmp_df1[col].astype(str).astype(df[col].dtype)
        out_df = pd.concat([df,tmp_df2],axis=0)
    else:
        out_df = df
    return out_df