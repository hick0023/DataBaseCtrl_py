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
  - SerchDict : Dict [str,str|int|float|Decimal|bool] 
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
順次メソッドの説明は作成します。