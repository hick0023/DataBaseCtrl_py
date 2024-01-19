# Access DataBaseを制御するクラス

データベースをクラス内のpandasデータフレームにロードして使用するデータフレームモードと直接データベースにアクセスするダイレクトモードがあります。

## 使用方法

### データフレームモード

```Sample Internal DataFrame mode
from DataBaseCtrl import DataBaseCtrl, SerchCondition, AccessDataType

# インスタンス
DataBase = DataBaseCtrl('DataBase File Path', 'TableName', False)

# クラス内データフレームにデータベースを読み込む
res = DataBase.UpdateInternalDataFrame()
```

### ダイレクトモード

```Sample Direct DataBase access mode
from DataBaseCtrl import DataBaseCtrl, SerchCondition, AccessDataType

# インスタンス
DataBase = DataBaseCtrl('DataBase File Path', 'TableName', True)
```

## メソッド

### クラス内データフレームをデータベースからアップデートする。（データフレームモードのみ）

``` UpdateInternalDataFrame()
res = DataBase.UpdateInternalDataFrame()
```

- Args
  - set_index (Optional[str])
    - インデクスにする行名
    - Noneとするとインデックス指定しない（非推奨）
    - Default="ID"
- Returns (bool)
  - 成功=True / 失敗=False

### 内部データフレームのコピーを取得する。（データフレームモードのみ）

```GetCopyInternalDataFrame
df = DataBase.GetCopyInternalDataFrame()
```

- Returns (pd.DataFrame)
  - 内部データフレームのコピー
  - 行状態が削除のものはコピーされない

### IDでデータフレームの行を検索（IDがKEYインデクスになっている場合）

```SelectRowByID()
df = DataBase.SelectRowByID(yourID)
```

- Args
  - ID (int | str)
    - 検索するID
  - Ext_DF (pd.DataFrame)
    - 検索する対象を外部入力のDataFrameにする。
    - Default = None : 外部を使わない
- Returns (pd.DataFrame)
  - 検索結果
  - ヒットしない場合、空のDataFrameを返す

### 検索条件で行を検索する

```SerchRows()
yourSerch:Dict[str,Any] = {"Col1":"AA","Col2":1,"Col3":2}
df = DataBase.SerchRows(yourSerch,
                        SerchCondition,
                        MultiSerch_Type,
                        Ext_DF)
```

- Args
  - SerchDict : Dict [str , str | int | float | Decimal | bool ]
    - 検索する行名と値のKey-Val Dictionary
    - 値は str | int | float | Decimal | bool (timedateを忘れているのでそのうち追加する)
  - SerchCondition : SerchCondition
    - SerchConditionクラスにEnumとして定義
    - Default = SerchCondition.Exact：完全一致、等しい
  - MultiSerch_Type : bool
    - 複数条件一括検索の場合AND検索かOR検索の選択
    - Default = True : AND検索
  - Ext_DF (pd.DataFrame)
    - 検索する対象を外部入力のDataFrameにする。
    - Default = None : 外部を使わない
- Returns : pd.DataFrame
  - 検索結果
  - ヒットしない場合、空のDataFrameを返す
- Remarks
  - 検索内容は同じ列名(Key)で複数条件はできません。絞り込み検索は、一度出た結果を外部データフレームとして検索してください。
- SerchConditionクラス
  
```SerchCondition Class
class SerchCondition(Enum):
"""検索条件"""
    Exact = 0           """完全一致、等しい、Boolでも適用可能"""
    StartWith = 1       """~で始まる"""    
    EndWith = 2         """~で終わる"""    
    Contains = 3        """~を含む"""
    SmallerThan = 4     """~より小さい"""
    OrSmallerThan = 5   """~以下"""
    LargerThan = 6      """~より大きい"""
    OrLargerThan = 7    """~以上"""  
```

### 行を更新する

```UpdateRow()
update_dict:Dict[str,Any] = {"col1","A","col2","B","col3","24"}
ID:int = 1
res = DataBase.UpdateRow(ID,update_dict)
```

UpdateRow(ID:Union[int,str], UpdateDict:Dict[str,Any]) -> bool:
内部データフレームまたはデータベースの行を更新（変更）する。

- Args:
  - ID (Union[int,str]): 変更する行のID
  - UpdateDict (Dict[str,Any]): 変更する内容<列名,変更後の値>
- Returns:
  - bool: 成功=True / 失敗=False
- Remarks:
  - データフレームモード: 内部データフレームが更新、データベースを更新（同期）させるまで変更されない。UpdateDataBase()
  - ダイレクトモード: データベースが直接更新される。

### 行を追加する

```AddRow()
add_dict:Dict[str,Any] = {"col1","A","col2","B","col3","24"}
ID:int = 1
res = DataBase.AddRow(add_dict, ID)
```

AddRow(AddDict:Dict[str,Any],ID:Union[int,str]=None) -> bool:
内部データフレームまたはデータベースに行を追加する。

- Args:
  - AddDict (Dict[str,Any]): 追加する内容<列名,値>
  - ID (Union[int,str], optional): ID、Noneで自動取得. Defaults to None.
- Returns:
  - bool: 成功=True / 失敗=False
- Remarks:
  - データフレームモード: 内部データフレームへ追加、データベースを更新（同期）させるまで変更されない。UpdateDataBase()
  - ダイレクトモード: データベースが直接追加される。

### 行を削除する

```DeleteRow()
ID:int = 1
res =DataBase.DeleteRow(ID)
```

DeleteRow(ID:Union[int,str], Del:bool=True) -> bool:
内部データフレームまたはデータベースの行を削除する(RowStateのみ変更)。

- Args:
  - ID (int, str): 削除する行ID
  - Del (bool, optional): 削除=True / 削除を解除=False、ダイレクトモードでは解除できない. Defaults to True.
- Returns:
  - bool: 成功=True / 失敗=False
- Remarks:
  - データフレームモード: 内部データフレームから削除、データベースを更新（同期）させるまで変更されないUpdateDataBase()。変更・追加した行は削除できない。一度データベースと同期をとった後削除してください。
  - ダイレクトモード: データベースから直接削除される。

### データベースを内部DataFrameで更新する（同期）

```UpdateDataBase()
res = DataBase.UpdateDataBase()
```

UpdateDataBase() -> bool:
データベースを内部DataFrameで更新する（同期）。ダイレクトモードでは動作しない。

- Returns:
  - bool: 成功=True / 失敗=False

### データベースへ列を追加する

```AddColumn_DataBase()
col_name:str = "A"
dtype:AccessDataType = AccessDataType.VARCHAR
param:list=[255]
res = DataBase.AddColumn_DataBase(col_name, dtype, param)
```

AddColumn_DataBase(ColmunName:str, DataType:AccessDataType, param_list:list=[]) -> bool:
データベースへ列を追加する。

- Args:
  - ColmunName (str): 追加する行名
  - DataType (AccessDataType): データ型
  - param_list (list, optional): パラメータ. Defaults to [].
- Returns:
  - bool: 成功=True / 失敗=False
- Remarks param_list:
  - DataType = CHAR: [max length]
  - DataType = VARCHAR: [max length]
  - (Not Available) DataType = DECIMAL: [total digits, digits after the decimal point]

データタイプ（Enum）

```class AccessDataType(Enum)
class AccessDataType(Enum)
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
```

### データベースから列を削除する

```DeleteColumn_DataBase()
col_name:str = "A"
res = DataBase.DeleteColumn_DataBase(col_name)
```

DeleteColumn_DataBase(ColumnName:str) -> bool:
データベースから列を削除する。

- Args:
  - ColumnName (str): 削除する列名
- Returns:
  - bool: 成功=True / 失敗=False
